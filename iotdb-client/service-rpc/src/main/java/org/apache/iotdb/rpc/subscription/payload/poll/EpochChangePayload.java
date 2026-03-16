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
 * Payload for {@link SubscriptionPollResponseType#EPOCH_CHANGE}.
 *
 * <p>Delivered by the old write-leader DataNode when it loses preferred-writer status for a region.
 * Signals that all data for the ending epoch has been dispatched. The client-side {@code
 * EpochOrderingProcessor} uses this to advance its epoch tracking and release buffered messages
 * from the next epoch.
 */
public class EpochChangePayload implements SubscriptionPollPayload {

  private transient long endingEpoch;

  public EpochChangePayload() {}

  public EpochChangePayload(final long endingEpoch) {
    this.endingEpoch = endingEpoch;
  }

  public long getEndingEpoch() {
    return endingEpoch;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(endingEpoch, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    endingEpoch = ReadWriteIOUtils.readLong(buffer);
    return this;
  }

  @Override
  public String toString() {
    return "EpochChangePayload{endingEpoch=" + endingEpoch + '}';
  }
}
