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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadUnavailableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * The replication request of one LOAD piece. While it waits in the replication queues it keeps the
 * piece metadata and the references of its chunk payloads, and it reads those payloads back from
 * the staged TsFile only when the bytes are actually needed for sending.
 *
 * <p>The WAL keeps the same reference form, so a replica forwarding a piece to a follower does not
 * keep a second copy of every chunk in memory while the entry waits to be replicated. The staged
 * file is guaranteed to be readable for as long as the piece may still be forwarded, because the
 * staged directory of a task is released only after the consensus watermark has passed its COMMIT
 * or ABORT command.
 *
 * <p>It stays an {@link IoTConsensusRequest} so that every consumer of a forwarded entry keeps
 * treating it exactly as before.
 */
public class LoadPieceConsensusRequest extends IoTConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadPieceConsensusRequest.class);

  private final LoadTsFileConsensusNode node;

  /**
   * The bytes this request retains while it waits to be sent: the piece metadata together with the
   * reference of every chunk payload, but no payload itself.
   */
  private final long retainedMemorySize;

  public LoadPieceConsensusRequest(final LoadTsFileConsensusNode node) {
    this(node, node.serialize(false));
  }

  private LoadPieceConsensusRequest(
      final LoadTsFileConsensusNode node, final ByteBuffer references) {
    super(references);
    this.node = node;
    this.retainedMemorySize = references.remaining();
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try {
      // Read the referenced payloads back from the staged TsFile and build the self-contained piece
      // the follower applies.
      return node.serialize();
    } catch (final ChunkPayloadUnavailableException e) {
      // The staged file may already be gone, for example after COMMIT released it. Degrade to the
      // reference-only form instead of failing: the follower then rejects the piece in its PREPARE
      // with the real reason, while the replication pipeline of this replica keeps running.
      LOGGER.warn(
          StorageEngineMessages
              .LOG_FAILED_TO_READ_BACK_THE_STAGED_CONTENT_OF_LOAD_PIECE_LOAD_ARG_PIECE_INDEX_ARG_SENDING_THE_REFERENCE_ONLY_D054333F,
          node.getLoadId(),
          node.getPieceIndex(),
          e);
      return node.serialize(false);
    }
  }

  @Override
  public long getMemorySize() {
    return retainedMemorySize;
  }

  /**
   * Returns the size {@link #serializeToByteBuffer()} produces, estimated without touching the
   * staged file. The WAL reader uses it to keep bounding how much it collects before handing the
   * entries over, exactly as it did when the bytes were materialized eagerly.
   */
  public long getSerializedSize() {
    return retainedMemorySize + node.getDataSize();
  }

  @Override
  public boolean isSerializationDeferred() {
    return true;
  }

  @Override
  public String toString() {
    return "LoadPieceConsensusRequest{loadId="
        + node.getLoadId()
        + ", tsFileId="
        + node.getTsFileId()
        + ", pieceIndex="
        + node.getPieceIndex()
        + ", retainedMemorySize="
        + retainedMemorySize
        + '}';
  }
}
