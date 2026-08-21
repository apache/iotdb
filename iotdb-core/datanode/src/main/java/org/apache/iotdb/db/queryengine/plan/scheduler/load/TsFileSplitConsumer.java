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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * LOAD split consumer: receives every {@link TsFileData} of one source TsFile's split output from
 * {@link TsFileSplitter} and routes -&gt; buffers -&gt; dispatches it as per-region consensus
 * pieces.
 *
 * <ul>
 *   <li>{@code CHUNK} data is buffered directionless first; before dispatch {@link
 *       DataPartitionRouter} resolves the target regions and {@link PieceDispatcher} appends every
 *       chunk to its per-region piece.
 *   <li>{@code DELETION} data is replicated into every buffered piece (chunks are routed first so a
 *       deletion never overtakes chunk data).
 *   <li>{@link MemoryBoundedBuffer} guards the shared data cache; over budget, the largest piece is
 *       dispatched immediately.
 *   <li>At end of file {@link #sendAllTsFileData()} flushes the remainder.
 * </ul>
 *
 * The pipeline also notifies the progress-index callback for every chunk's time partition, so the
 * strategy can track pipe progress while splitting. {@link #clear()} releases all buffered
 * accounting and piece references.
 */
public class TsFileSplitConsumer implements TsFileSplitter.TsFileDataConsumer {

  private final LoadSingleTsFileNode singleTsFileNode;
  private final DataPartitionRouter router;
  private final MemoryBoundedBuffer memoryBuffer;
  private final PieceDispatcher dispatcher;
  private final Consumer<TTimePartitionSlot> progressIndexCallback;

  private final List<ChunkData> nonDirectionalChunkData = new ArrayList<>();

  public TsFileSplitConsumer(
      LoadSingleTsFileNode singleTsFileNode,
      LoadTsFileDataCacheMemoryBlock block,
      DataPartitionBatchFetcher partitionFetcher,
      String userName,
      Consumer<TTimePartitionSlot> progressIndexCallback,
      PieceDispatcher.DispatchCallback dispatchCallback) {
    this.singleTsFileNode = singleTsFileNode;
    this.router = new DataPartitionRouter(partitionFetcher, userName);
    this.memoryBuffer = new MemoryBoundedBuffer(block);
    this.dispatcher = new PieceDispatcher(singleTsFileNode, memoryBuffer, dispatchCallback);
    this.progressIndexCallback = progressIndexCallback;
  }

  @Override
  public boolean apply(TsFileData tsFileData) throws LoadFileException {
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

  private boolean addOrSendChunkData(ChunkData chunkData) throws LoadFileException {
    nonDirectionalChunkData.add(chunkData);
    memoryBuffer.add(chunkData.getDataSize());
    progressIndexCallback.accept(chunkData.getTimePartitionSlot());

    if (!memoryBuffer.isMemoryEnough()) {
      routeChunkData();
      if (!dispatcher.dispatchLargestUntilMemoryEnough()) {
        return false;
      }
    }
    return true;
  }

  private boolean addOrSendDeletionData(DeletionData deletionData) throws LoadFileException {
    routeChunkData(); // ensure chunk data will be added before deletion
    dispatcher.addDeletionToAll(deletionData);
    return true;
  }

  private void routeChunkData() throws LoadFileException {
    if (nonDirectionalChunkData.isEmpty()) {
      return;
    }

    final List<TRegionReplicaSet> replicaSets = router.route(nonDirectionalChunkData);
    for (int i = 0, size = nonDirectionalChunkData.size(); i < size; i++) {
      dispatcher.offerChunk(nonDirectionalChunkData.get(i), replicaSets.get(i));
    }
    nonDirectionalChunkData.clear();
  }

  boolean sendAllTsFileData() throws LoadFileException {
    routeChunkData();
    return dispatcher.flushAll();
  }

  /** Last-chance cleanup: returns all buffered accounting and drops every piece reference. */
  void clear() {
    memoryBuffer.clear();
    nonDirectionalChunkData.clear();
    dispatcher.clear();
  }
}
