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

package org.apache.iotdb.db.mpp.plan.execution.config;

<<<<<<< HEAD
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.db.client.ConfigNodeClient;

=======
>>>>>>> abbe779bb7 (add interface)
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.db.mpp.plan.execution.config.fetcher.IConfigTaskFetcher;

public interface IConfigTask {
<<<<<<< HEAD
  ListenableFuture<ConfigTaskResult> execute(
<<<<<<< HEAD
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
=======
      IClientManager<PartitionRegionId, DataNodeToConfigNodeClient> clientManager)
=======
  ListenableFuture<ConfigTaskResult> execute(IConfigTaskFetcher configTaskFetcher)
>>>>>>> abbe779bb7 (add interface)
>>>>>>> 8e3e7554e5 (add interface)
      throws InterruptedException;
}
