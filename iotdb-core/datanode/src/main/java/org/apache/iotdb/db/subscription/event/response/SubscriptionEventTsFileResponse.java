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

package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.db.subscription.event.cache.SubscriptionPollResponseCache;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class SubscriptionEventTsFileResponse
    extends SubscriptionEventExtendableResponse<CachedSubscriptionPollResponse> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTsFileResponse.class);

  private final File tsFile;
  private final SubscriptionCommitContext commitContext;

  private volatile boolean isSealed = false;

  public SubscriptionEventTsFileResponse(
      final File tsFile, final SubscriptionCommitContext commitContext) {
    super();

    this.tsFile = tsFile;
    this.commitContext = commitContext;

    offerInitialResponse();
  }

  private void offerInitialResponse() {
    if (!isEmpty()) {
      LOGGER.warn("broken invariant");
      return;
    }
    offer(
        new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            new FileInitPayload(tsFile.getName()),
            commitContext));
  }

  @Override
  public CachedSubscriptionPollResponse getCurrentResponse() {
    return peekFirst();
  }

  @Override
  public void prefetchRemainingResponses() throws IOException {
    synchronized (this) {
      final SubscriptionPollResponse previousResponse = peekLast();
      if (Objects.isNull(previousResponse)) {
        LOGGER.warn("broken invariant");
        return;
      }
      final short responseType = previousResponse.getResponseType();
      final SubscriptionPollPayload payload = previousResponse.getPayload();
      if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
        LOGGER.warn("unexpected response type: {}", responseType);
        return;
      }

      switch (SubscriptionPollResponseType.valueOf(responseType)) {
        case FILE_INIT:
          offer(generateResponseWithPieceOrSealPayload(0));
          break;
        case FILE_PIECE:
          offer(
              generateResponseWithPieceOrSealPayload(
                  ((FilePiecePayload) payload).getNextWritingOffset()));
          break;
        case FILE_SEAL:
          // not need to prefetch
          break;
        default:
          LOGGER.warn("unexpected message type: {}", responseType);
      }
    }
  }

  @Override
  public void fetchNextResponse() throws IOException {
    prefetchRemainingResponses();
    if (Objects.isNull(poll())) {
      LOGGER.warn("broken invariant");
    }
  }

  @Override
  public void trySerializeCurrentResponse() {
    SubscriptionPollResponseCache.getInstance().trySerialize(getCurrentResponse());
  }

  @Override
  public void trySerializeRemainingResponses() {
    stream()
        .filter(response -> Objects.isNull(response.getByteBuffer()))
        .findFirst()
        .ifPresent(response -> SubscriptionPollResponseCache.getInstance().trySerialize(response));
  }

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return SubscriptionPollResponseCache.getInstance().serialize(getCurrentResponse());
  }

  @Override
  public void resetCurrentResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance().invalidate(getCurrentResponse());
  }

  @Override
  public void reset() {
    cleanUp();
    offerInitialResponse();
  }

  @Override
  public void cleanUp() {
    CachedSubscriptionPollResponse response;
    while (Objects.nonNull(response = poll())) {
      SubscriptionPollResponseCache.getInstance().invalidate(response);
    }
    isSealed = false;
  }

  @Override
  public boolean isCommittable() {
    return isSealed && size() == 1;
  }

  /////////////////////////////// utility ///////////////////////////////

  private @NonNull CachedSubscriptionPollResponse generateResponseWithPieceOrSealPayload(
      final long writingOffset) throws IOException {
    final long readFileBufferSize =
        SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
    final byte[] readBuffer = new byte[(int) readFileBufferSize];
    try (final RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      while (true) {
        reader.seek(writingOffset);
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] filePiece =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);

        // generate subscription poll response with piece payload
        return new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_PIECE.getType(),
            new FilePiecePayload(tsFile.getName(), writingOffset + readLength, filePiece),
            commitContext);
      }

      // generate subscription poll response with seal payload
      isSealed = true;
      return new CachedSubscriptionPollResponse(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          new FileSealPayload(tsFile.getName(), tsFile.length()),
          commitContext);
    }
  }
}
