package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SubscriptionEventTabletResponse extends SubscriptionEventExtendableResponse {

  public SubscriptionEventTabletResponse(
      final List<Tablet> tablets, final SubscriptionCommitContext commitContext) {}

  @Override
  public SubscriptionPollResponse getCurrentResponse() {
    return null;
  }

  @Override
  public void prefetchRemainingResponses() throws IOException {}

  @Override
  public void fetchNextResponse() throws IOException {}

  @Override
  public void trySerializeCurrentResponse() {}

  @Override
  public void trySerializeRemainingResponses() {}

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return null;
  }

  @Override
  public void resetResponseByteBuffer() {}

  @Override
  public void reset() {}

  @Override
  public void cleanUp() {}

  @Override
  public boolean isCommittable() {
    return false;
  }
}
