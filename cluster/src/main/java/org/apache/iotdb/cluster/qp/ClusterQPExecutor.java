/**
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
package org.apache.iotdb.cluster.qp;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;

public abstract class ClusterQPExecutor {

  /**
   * Get Storage Group Name by device name
   */
  public String getStroageGroupByDevice(String device) throws PathErrorException {
    String storageGroup;
    try {
      storageGroup = MManager.getInstance().getFileNameByPath(device);
    } catch (PathErrorException e) {
      throw new PathErrorException(String.format("File level of {} doesn't exist.", device));
    }
    return storageGroup;
  }

  /**
   * Get raft group id by storage group name
   */
  public String getGroupIdBySG(String storageGroup) {
    return null;
  }

  /**
   * Verify if the command can execute in local.
   */
  public boolean canHandle(String storageGroup) {
    return false;
  }

  /**
   * Get metadata raft group id
   */
  public String getMetadataGroupID() {
    return null;
  }
}
