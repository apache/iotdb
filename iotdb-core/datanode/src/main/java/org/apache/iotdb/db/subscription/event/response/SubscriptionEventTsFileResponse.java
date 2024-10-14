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
  private boolean isInitialResponseConsumed = false;

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
    if (!isInitialResponseConsumed) {
      super.responses.add(generateResponseWithPieceOrSealPayload(0));
    } else {
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
          return;
        case FILE_INIT: // fallthrough
        default:
          LOGGER.warn("unexpected message type: {}", responseType);
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
  public void resetResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance()
        .invalidate((CachedSubscriptionPollResponse) getCurrentResponse());
  }

  @Override
  public void reset() {
    super.responses.clear();
    isInitialResponseConsumed = false;
  }

  @Override
  public void cleanUp() {}

  @Override
  public boolean isCommittable() {
    return false;
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
      return new CachedSubscriptionPollResponse(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          new FileSealPayload(tsFile.getName(), tsFile.length()),
          commitContext);
    }
  }
}
