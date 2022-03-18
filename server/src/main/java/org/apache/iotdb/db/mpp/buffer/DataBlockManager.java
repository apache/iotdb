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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;

import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class DataBlockManager {

  public static class FragmentInstanceInfo {
    private String hostname;
    private String queryId;
    private String fragmentId;
    private String instanceId;

    public FragmentInstanceInfo(
        String hostname, String queryId, String fragmentId, String instanceId) {
      this.hostname = Validate.notNull(hostname);
      this.queryId = Validate.notNull(queryId);
      this.fragmentId = Validate.notNull(fragmentId);
      this.instanceId = Validate.notNull(instanceId);
    }

    public String getHostname() {
      return hostname;
    }

    public String getQueryId() {
      return queryId;
    }

    public String getFragmentId() {
      return fragmentId;
    }

    public String getInstanceId() {
      return instanceId;
    }
  }

  /**
   * Create a sink handle.
   *
   * @param local The {@link FragmentInstanceInfo} of local fragment instance.
   * @param remote The {@link FragmentInstanceInfo} of downstream instance.
   */
  public ISinkHandle createSinkHandle(FragmentInstanceInfo local, FragmentInstanceInfo remote) {
    throw new UnsupportedOperationException();
  }

  public ISinkHandle createPartitionedSinkHandle(
      FragmentInstanceInfo local, List<FragmentInstanceInfo> remotes) {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a source handle.
   *
   * @param local The {@link FragmentInstanceInfo} of local fragment instance.
   * @param remote The {@link FragmentInstanceInfo} of downstream instance.
   */
  public ISourceHandle createSourceHandle(FragmentInstanceInfo local, FragmentInstanceInfo remote) {
    throw new UnsupportedOperationException();
  }

  /**
   * Release all the related resources, including data blocks that are not yet sent to downstream
   * fragment instances.
   *
   * <p>This method should be called when a fragment instance finished in an abnormal state.
   */
  void forceDeregisterFragmentInstance(FragmentInstanceTask task) {
    throw new UnsupportedOperationException();
  }

  LocalMemoryManager localMemoryManager;
  ScheduledExecutorService scheduledExecutorService;
  DataBlockServiceClientFactory clientFactory;
  Map<String, Map<String, Map<String, ISourceHandle>>> sourceHandles;
  Map<String, Map<String, Map<String, ISinkHandle>>> sinkHandles;

  public DataBlockManager(LocalMemoryManager localMemoryManager) {
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    // TODO: configurable number of threads
    scheduledExecutorService =
        IoTDBThreadPoolFactory.newScheduledThreadPoolWithDaemon(5, "get-data-block");
    clientFactory = new DataBlockServiceClientFactory();
    sourceHandles = new HashMap<>();
    sinkHandles = new HashMap<>();
  }
}
