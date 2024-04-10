package org.apache.iotdb.db.subscription.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import java.util.List;

public class TsFileSubscriptionEvent extends SubscriptionEvent {

  public TsFileSubscriptionEvent(List<EnrichedEvent> enrichedEvents, String subscriptionCommitId) {
    super(enrichedEvents, subscriptionCommitId);
  }
}
