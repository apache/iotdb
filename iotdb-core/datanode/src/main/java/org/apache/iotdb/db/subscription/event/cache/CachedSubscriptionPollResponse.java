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

package org.apache.iotdb.db.subscription.event.cache;

import org.apache.iotdb.db.pipe.resource.memory.PipeFixedMemoryBlock;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class CachedSubscriptionPollResponse extends SubscriptionPollResponse {

  private volatile ByteBuffer byteBuffer; // cached serialized response

  private volatile PipeFixedMemoryBlock memoryBlock;

  public CachedSubscriptionPollResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    super(responseType, payload, commitContext);
  }

  public CachedSubscriptionPollResponse(final SubscriptionPollResponse response) {
    super(response.getResponseType(), response.getPayload(), response.getCommitContext());
  }

  public void setMemoryBlock(final PipeFixedMemoryBlock memoryBlock) {
    this.memoryBlock = memoryBlock;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public void invalidateByteBuffer() {
    // maybe friendly for gc
    byteBuffer = null;
    if (Objects.nonNull(memoryBlock)) {
      memoryBlock.close();
    }
  }

  public static ByteBuffer serialize(final CachedSubscriptionPollResponse response)
      throws IOException {
    return response.serialize();
  }

  private ByteBuffer serialize() throws IOException {
    return Objects.nonNull(byteBuffer)
        ? byteBuffer
        : (byteBuffer = SubscriptionPollResponse.serialize(this));
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "CachedSubscriptionPollResponse" + coreReportMessage();
  }

  @Override
  protected Map<String, String> coreReportMessage() {
    final Map<String, String> coreReportMessage = super.coreReportMessage();
    coreReportMessage.put(
        "sizeof(byteBuffer)",
        Objects.isNull(byteBuffer)
            ? "<unknown>"
            : String.valueOf(byteBuffer.limit() - byteBuffer.position()));
    return coreReportMessage;
  }
}
