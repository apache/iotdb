package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;

import java.util.LinkedList;

public abstract class SubscriptionEventExtendableResponse implements SubscriptionEventResponse {

  protected final LinkedList<CachedSubscriptionPollResponse> responses;

  protected SubscriptionEventExtendableResponse() {
    this.responses = new LinkedList<>();
  }
}
