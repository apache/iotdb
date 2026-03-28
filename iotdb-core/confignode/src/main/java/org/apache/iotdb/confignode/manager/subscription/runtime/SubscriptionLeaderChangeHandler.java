package org.apache.iotdb.confignode.manager.subscription.runtime;

import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;

public class SubscriptionLeaderChangeHandler implements IClusterStatusSubscriber {

  private final SubscriptionRuntimeCoordinator runtimeCoordinator;

  public SubscriptionLeaderChangeHandler(final SubscriptionRuntimeCoordinator runtimeCoordinator) {
    this.runtimeCoordinator = runtimeCoordinator;
  }

  @Override
  public void onNodeStatisticsChanged(final NodeStatisticsChangeEvent event) {
    runtimeCoordinator.handleNodeStatisticsChange(event);
  }

  @Override
  public void onConsensusGroupStatisticsChanged(final ConsensusGroupStatisticsChangeEvent event) {
    runtimeCoordinator.handleLeaderChangeEvent(event);
  }
}
