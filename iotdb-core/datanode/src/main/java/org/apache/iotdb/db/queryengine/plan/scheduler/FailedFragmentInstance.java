package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;

public class FailedFragmentInstance {
  private final FragmentInstance instance;
  private final TSStatus failureStatus;

  public FailedFragmentInstance(FragmentInstance instance, TSStatus failureStatus) {
    this.instance = instance;
    this.failureStatus = failureStatus;
  }

  public FragmentInstance getInstance() {
    return instance;
  }

  public TSStatus getFailureStatus() {
    return failureStatus;
  }
}
