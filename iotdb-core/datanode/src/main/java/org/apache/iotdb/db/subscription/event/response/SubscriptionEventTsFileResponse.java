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
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The {@code SubscriptionEventTsFileResponse} class extends {@link
 * SubscriptionEventExtendableResponse} to manage subscription responses related to time series
 * files. The actual payload can include {@link FileInitPayload}, {@link FilePiecePayload}, and
 * {@link FileSealPayload}, allowing for detailed control over file data streaming.
 */
public class SubscriptionEventTsFileResponse extends SubscriptionEventExtendableResponse {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTsFileResponse.class);

  private final File tsFile;
  private final SubscriptionCommitContext commitContext;

  public SubscriptionEventTsFileResponse(
      final File tsFile, final SubscriptionCommitContext commitContext) {
    super();

    this.tsFile = tsFile;
    this.commitContext = commitContext;

    init();
  }

  @Override
  public void prefetchRemainingResponses() throws IOException {
    if (hasNoMore) {
      return;
    }

    generateNextTsFileResponse().ifPresent(super::offer);
  }

  @Override
  public void nack() {
    cleanUp();
    init();
  }

  @Override
  public void cleanUp() {
    super.cleanUp();
  }

  /////////////////////////////// utility ///////////////////////////////

  private void init() {
    if (!isEmpty()) {
      LOGGER.warn(
          "SubscriptionEventTsFileResponse {} is not empty when initializing (broken invariant)",
          this);
      return;
    }

    offer(
        new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            new FileInitPayload(tsFile.getName()),
            commitContext));
  }

  private synchronized Optional<CachedSubscriptionPollResponse> generateNextTsFileResponse()
      throws IOException {
    final SubscriptionPollResponse previousResponse = peekLast();
    if (Objects.isNull(previousResponse)) {
      LOGGER.warn(
          "SubscriptionEventTsFileResponse {} is empty when generating next response (broken invariant)",
          this);
      return Optional.empty();
    }
    final short responseType = previousResponse.getResponseType();
    final SubscriptionPollPayload payload = previousResponse.getPayload();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      LOGGER.warn("unexpected response type: {}", responseType);
      return Optional.empty();
    }

    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case FILE_INIT:
        return Optional.of(generateResponseWithPieceOrSealPayload(0));
      case FILE_PIECE:
        return Optional.of(
            generateResponseWithPieceOrSealPayload(
                ((FilePiecePayload) payload).getNextWritingOffset()));
      case FILE_SEAL:
        // not need to prefetch
        break;
      default:
        LOGGER.warn("unexpected message type: {}", responseType);
    }

    return Optional.empty();
  }

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
      hasNoMore = true;
      return new CachedSubscriptionPollResponse(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          new FileSealPayload(tsFile.getName(), tsFile.length()),
          commitContext);
    }
  }
}
