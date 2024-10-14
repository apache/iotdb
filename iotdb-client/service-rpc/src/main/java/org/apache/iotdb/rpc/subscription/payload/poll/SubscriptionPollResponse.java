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

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SubscriptionPollResponse {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPollResponse.class);

  private final transient short responseType;

  private final transient SubscriptionPollPayload payload;

  private final transient SubscriptionCommitContext commitContext;

  public SubscriptionPollResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    this.responseType = responseType;
    this.payload = payload;
    this.commitContext = commitContext;
  }

  public short getResponseType() {
    return responseType;
  }

  public SubscriptionPollPayload getPayload() {
    return payload;
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(final SubscriptionPollResponse response) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      response.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(responseType, stream);
    payload.serialize(stream);
    commitContext.serialize(stream);
  }

  public static SubscriptionPollResponse deserialize(final ByteBuffer buffer) {
    final short responseType = ReadWriteIOUtils.readShort(buffer);
    SubscriptionPollPayload payload = null;
    if (SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      switch (SubscriptionPollResponseType.valueOf(responseType)) {
        case TABLETS:
          payload = new TabletsPayload().deserialize(buffer);
          break;
        case FILE_INIT:
          payload = new FileInitPayload().deserialize(buffer);
          break;
        case FILE_PIECE:
          payload = new FilePiecePayload().deserialize(buffer);
          break;
        case FILE_SEAL:
          payload = new FileSealPayload().deserialize(buffer);
          break;
        case ERROR:
          payload = new ErrorPayload().deserialize(buffer);
          break;
        case TERMINATION:
          payload = new TerminationPayload().deserialize(buffer);
          break;
        default:
          LOGGER.warn("unexpected response type: {}, payload will be null", responseType);
          break;
      }
    } else {
      LOGGER.warn("unexpected response type: {}, payload will be null", responseType);
    }

    final SubscriptionCommitContext commitContext = SubscriptionCommitContext.deserialize(buffer);
    return new SubscriptionPollResponse(responseType, payload, commitContext);
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPollResponse" + coreReportMessage();
  }

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("responseType", SubscriptionPollResponseType.valueOf(responseType).toString());
    result.put("payload", payload.toString());
    result.put("commitContext", commitContext.toString());
    return result;
  }
}
