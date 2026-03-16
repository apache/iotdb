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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Payload for {@link SubscriptionPollResponseType#WATERMARK}.
 *
 * <p>Periodically injected by the server-side {@code ConsensusPrefetchingQueue} to report timestamp
 * progress for a region. Carries the maximum data timestamp observed so far, enabling client-side
 * {@code WatermarkProcessor} to advance its watermark even when a region is idle (no new data).
 *
 * <p>The {@code dataNodeId} identifies which DataNode emitted this watermark, allowing the client
 * to track per-node progress across leader transitions.
 */
public class WatermarkPayload implements SubscriptionPollPayload {

  /** Maximum data timestamp observed across all InsertNodes in this region's queue. */
  private transient long watermarkTimestamp;

  /** The DataNode ID that emitted this watermark. */
  private transient int dataNodeId;

  public WatermarkPayload() {}

  public WatermarkPayload(final long watermarkTimestamp, final int dataNodeId) {
    this.watermarkTimestamp = watermarkTimestamp;
    this.dataNodeId = dataNodeId;
  }

  public long getWatermarkTimestamp() {
    return watermarkTimestamp;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(watermarkTimestamp, stream);
    ReadWriteIOUtils.write(dataNodeId, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    watermarkTimestamp = ReadWriteIOUtils.readLong(buffer);
    dataNodeId = ReadWriteIOUtils.readInt(buffer);
    return this;
  }

  @Override
  public String toString() {
    return "WatermarkPayload{watermarkTimestamp="
        + watermarkTimestamp
        + ", dataNodeId="
        + dataNodeId
        + '}';
  }
}
