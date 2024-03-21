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

package org.apache.iotdb.db.subscription.execution.executor;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;

public class SubscriptionSubtaskExecutor extends PipeConnectorSubtaskExecutor {

  public SubscriptionSubtaskExecutor() {
    super(
        SubscriptionConfig.getInstance().getSubscriptionSubtaskExecutorMaxThreadNum(),
        ThreadName.SUBSCRIPTION_EXECUTOR_POOL);
  }
}
