package org.apache.iotdb.db.subscription.execution.executor;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.pipe.execution.executor.PipeSubtaskExecutor;

public class SubscriptionSubtaskExecutor extends PipeSubtaskExecutor {

  public SubscriptionSubtaskExecutor() {
    super(
        4, // TODO: config
        ThreadName.SUBSCRIPTION_EXECUTOR_POOL);
  }
}
