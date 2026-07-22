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
import java.util.Objects;

public class PollTabletsPayload implements SubscriptionPollPayload {

  /** The commit context associated with the {@link SubscriptionPollResponse}. */
  private transient SubscriptionCommitContext commitContext;

  /** The index for the next batch of tablets. */
  private transient int offset;

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public int getOffset() {
    return offset;
  }

  public PollTabletsPayload() {}

  public PollTabletsPayload(final SubscriptionCommitContext commitContext, final int offset) {
    this.commitContext = commitContext;
    this.offset = offset;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    commitContext.serialize(stream);
    ReadWriteIOUtils.write(offset, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    commitContext = SubscriptionCommitContext.deserialize(buffer);
    offset = ReadWriteIOUtils.readInt(buffer);
    return this;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PollTabletsPayload that = (PollTabletsPayload) obj;
    return Objects.equals(this.commitContext, that.commitContext)
        && Objects.equals(this.offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitContext, offset);
  }

  @Override
  public String toString() {
    return "PollTabletsPayload{commitContext=" + commitContext + ", offset=" + offset + "}";
  }
}
