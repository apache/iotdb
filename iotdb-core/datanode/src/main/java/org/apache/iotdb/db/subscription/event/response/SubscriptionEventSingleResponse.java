package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.db.subscription.event.cache.SubscriptionPollResponseCache;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SubscriptionEventSingleResponse implements SubscriptionEventResponse {

  private final CachedSubscriptionPollResponse response;

  public SubscriptionEventSingleResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    this.response = new CachedSubscriptionPollResponse(responseType, payload, commitContext);
  }

  @Override
  public SubscriptionPollResponse getCurrentResponse() {
    return response;
  }

  @Override
  public void prefetchRemainingResponses() {
    // do nothing
  }

  @Override
  public void fetchNextResponse() {
    // do nothing
  }

  @Override
  public void trySerializeCurrentResponse() {
    SubscriptionPollResponseCache.getInstance().trySerialize(response);
  }

  @Override
  public void trySerializeRemainingResponses() {
    // do nothing
  }

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return SubscriptionPollResponseCache.getInstance().serialize(response);
  }

  @Override
  public void resetResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance().invalidate(response);
  }

  @Override
  public void reset() {
    // do nothing
  }
}
