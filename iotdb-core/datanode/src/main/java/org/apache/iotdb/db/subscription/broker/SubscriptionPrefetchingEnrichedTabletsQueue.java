package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;

public class SubscriptionPrefetchingEnrichedTabletsQueue extends SubscriptionPrefetchingQueue {

  public SubscriptionPrefetchingEnrichedTabletsQueue(
      String brokerId, String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);
  }
}
