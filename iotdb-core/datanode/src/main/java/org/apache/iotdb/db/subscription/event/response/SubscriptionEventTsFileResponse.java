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
import java.util.ConcurrentModificationException;
import java.util.Objects;

public class SubscriptionEventTsFileResponse extends SubscriptionEventExtendableResponse {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTsFileResponse.class);

  private final File tsFile;
  private final SubscriptionCommitContext commitContext;
  private final CachedSubscriptionPollResponse initialResponse;

  private volatile boolean isInitialResponseConsumed = false;
  private volatile boolean isSealed = false;

  public SubscriptionEventTsFileResponse(
      final File tsFile, final SubscriptionCommitContext commitContext) {
    super();

    this.tsFile = tsFile;
    this.commitContext = commitContext;
    this.initialResponse =
        new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            new FileInitPayload(tsFile.getName()),
            commitContext);
  }

  @Override
  public SubscriptionPollResponse getCurrentResponse() {
    if (!isInitialResponseConsumed) {
      return initialResponse;
    }
    return super.responses.getFirst();
  }

  @Override
  public void prefetchRemainingResponses() throws IOException {
    if (!isInitialResponseConsumed && super.responses.isEmpty()) {
      super.responses.add(generateResponseWithPieceOrSealPayload(0));
    } else {
      if (super.responses.isEmpty()) {
        LOGGER.warn("broken invariant");
        return;
      }

      synchronized (this) {
        final SubscriptionPollResponse previousResponse = super.responses.getLast();
        final short responseType = previousResponse.getResponseType();
        final SubscriptionPollPayload payload = previousResponse.getPayload();
        if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
          LOGGER.warn("unexpected response type: {}", responseType);
          return;
        }

        switch (SubscriptionPollResponseType.valueOf(responseType)) {
          case FILE_PIECE:
            super.responses.add(
                generateResponseWithPieceOrSealPayload(
                    ((FilePiecePayload) payload).getNextWritingOffset()));
            break;
          case FILE_SEAL:
            // not need to prefetch
            break;
          case FILE_INIT: // fallthrough
          default:
            LOGGER.warn("unexpected message type: {}", responseType);
        }
      }
    }
  }

  @Override
  public void fetchNextResponse() throws IOException {
    prefetchRemainingResponses();
    if (!isInitialResponseConsumed) {
      isInitialResponseConsumed = true;
    } else {
      if (super.responses.isEmpty()) {
        LOGGER.warn("broken invariant");
      } else {
        super.responses.removeFirst();
      }
    }
  }

  @Override
  public void trySerializeCurrentResponse() {
    SubscriptionPollResponseCache.getInstance()
        .trySerialize((CachedSubscriptionPollResponse) getCurrentResponse());
  }

  @Override
  public void trySerializeRemainingResponses() {
    if (!isInitialResponseConsumed) {
      if (Objects.isNull(initialResponse.getByteBuffer())) {
        SubscriptionPollResponseCache.getInstance().trySerialize(initialResponse);
        return;
      }
    }

    try {
      for (final CachedSubscriptionPollResponse response : super.responses) {
        if (Objects.isNull(response.getByteBuffer())) {
          SubscriptionPollResponseCache.getInstance().trySerialize(response);
          return;
        }
      }
    } catch (final ConcurrentModificationException e) {
      e.printStackTrace();
    }
  }

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return SubscriptionPollResponseCache.getInstance()
        .serialize((CachedSubscriptionPollResponse) getCurrentResponse());
  }

  @Override
  public void resetCurrentResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance()
        .invalidate((CachedSubscriptionPollResponse) getCurrentResponse());
  }

  @Override
  public void reset() {
    CachedSubscriptionPollResponse response;
    while (!super.responses.isEmpty()
        && Objects.nonNull(response = super.responses.removeFirst())) {
      SubscriptionPollResponseCache.getInstance().invalidate(response);
    }
    isInitialResponseConsumed = false;
    isSealed = false;
  }

  @Override
  public void cleanUp() {
    CachedSubscriptionPollResponse response;
    while (!super.responses.isEmpty()
        && Objects.nonNull(response = super.responses.removeFirst())) {
      SubscriptionPollResponseCache.getInstance().invalidate(response);
    }
    SubscriptionPollResponseCache.getInstance().invalidate(initialResponse);
    isInitialResponseConsumed = false;
    isSealed = false;
  }

  @Override
  public boolean isCommittable() {
    return isSealed && super.responses.size() == 1;
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
