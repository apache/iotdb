/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Split consumer that receives parsed chunk/deletion data from {@link TsFileSplitter}, resolves
 * target data regions, buffers chunks within memory limits, and drives piece dispatch.
 */
public class TsFileSplitConsumer implements TsFileSplitter.TsFileDataConsumer {

  private final LoadSingleTsFileNode singleTsFileNode;
  private final DataPartitionRouter router;
  private final MemoryBoundedBuffer memoryBuffer;
  private final PieceDispatcher dispatcher;
  private final Consumer<TTimePartitionSlot> progressIndexCallback;

  private final List<ChunkData> nonDirectionalChunkData = new ArrayList<>();

  public TsFileSplitConsumer(
      final LoadSingleTsFileNode singleTsFileNode,
      final LoadTsFileDataCacheMemoryBlock block,
      final DataPartitionBatchFetcher partitionFetcher,
      final String userName,
      final Consumer<TTimePartitionSlot> progressIndexCallback,
      final PieceDispatcher.DispatchCallback dispatchCallback) {
    this.singleTsFileNode =
        Objects.requireNonNull(
            singleTsFileNode,
            DataNodeQueryMessages.EXCEPTION_SINGLETSFILENODE_CANNOT_BE_NULL_4EA6CF51);
    Objects.requireNonNull(block, DataNodeQueryMessages.EXCEPTION_BLOCK_CANNOT_BE_NULL_7E31451D);
    Objects.requireNonNull(
        partitionFetcher, DataNodeQueryMessages.EXCEPTION_PARTITIONFETCHER_CANNOT_BE_NULL_5B709CA8);
    Objects.requireNonNull(
        dispatchCallback, DataNodeQueryMessages.EXCEPTION_DISPATCHCALLBACK_CANNOT_BE_NULL_C7A1AC6A);

    this.router = new DataPartitionRouter(partitionFetcher, userName);
    this.memoryBuffer = new MemoryBoundedBuffer(block);
    this.dispatcher = new PieceDispatcher(singleTsFileNode, memoryBuffer, dispatchCallback);
    this.progressIndexCallback = progressIndexCallback;
  }

  // -------------------------------------------------------------------------
  // Ingestion Pipeline
  // -------------------------------------------------------------------------

  @Override
  public boolean apply(final TsFileData tsFileData) throws LoadFileException {
    Objects.requireNonNull(
        tsFileData, DataNodeQueryMessages.EXCEPTION_TSFILEDATA_CANNOT_BE_NULL_EE1DDEC2);

    return switch (tsFileData.getType()) {
      case CHUNK -> addOrSendChunkData((ChunkData) tsFileData);
      case DELETION -> addOrSendDeletionData((DeletionData) tsFileData);
      default ->
          throw new UnsupportedOperationException(
              String.format(
                  DataNodeQueryMessages.QUERY_EXCEPTION_UNSUPPORTED_TSFILEDATATYPE_S_374475FA,
                  tsFileData.getType()));
    };
  }

  private boolean addOrSendChunkData(final ChunkData chunkData) throws LoadFileException {
    nonDirectionalChunkData.add(chunkData);
    memoryBuffer.add(chunkData.getDataSize());

    if (progressIndexCallback != null) {
      progressIndexCallback.accept(chunkData.getTimePartitionSlot());
    }

    if (!memoryBuffer.isMemoryEnough()) {
      routeChunkData();
      if (!dispatcher.dispatchLargestUntilMemoryEnough()) {
        return false;
      }
    }
    return true;
  }

  private boolean addOrSendDeletionData(final DeletionData deletionData) throws LoadFileException {
    // Route pending chunks first to ensure deletions never precede the chunks they apply to
    routeChunkData();
    dispatcher.addDeletionToAll(deletionData);
    return true;
  }

  // -------------------------------------------------------------------------
  // Routing & Dispatch Triggers
  // -------------------------------------------------------------------------

  /**
   * Resolves target replica sets for all directionless chunks and transfers them to the dispatcher.
   */
  private void routeChunkData() throws LoadFileException {
    if (nonDirectionalChunkData.isEmpty()) {
      return;
    }

    final List<TRegionReplicaSet> replicaSets = router.route(nonDirectionalChunkData);
    final int size = nonDirectionalChunkData.size();

    try {
      for (int i = 0; i < size; i++) {
        dispatcher.offerChunk(nonDirectionalChunkData.get(i), replicaSets.get(i));
      }
    } catch (final IOException e) {
      throw new LoadFileException(
          DataNodeQueryMessages.EXCEPTION_FAILED_TO_OFFER_CHUNK_TO_DISPATCHER_0300D5DD, e);
    } finally {
      nonDirectionalChunkData.clear();
    }
  }

  /** Flushes all remaining buffered chunks and deletions at the end of the source TsFile. */
  boolean sendAllTsFileData() throws LoadFileException {
    routeChunkData();
    return dispatcher.flushAll();
  }

  // -------------------------------------------------------------------------
  // Teardown
  // -------------------------------------------------------------------------

  /** Drops all unrouted chunks, clears partition buffers, and releases cached memory accounting. */
  void clear() {
    nonDirectionalChunkData.clear();
    dispatcher.clear();
    memoryBuffer.clear();
  }
}
