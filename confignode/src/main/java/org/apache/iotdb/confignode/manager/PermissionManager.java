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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.persistence.AuthorInfo;

import java.util.List;

/** manager permission query and operation */
public class PermissionManager {

  private final Manager configManager;
  private final AuthorInfo authorInfo;

  public PermissionManager(Manager configManager, AuthorInfo authorInfo) {
    this.configManager = configManager;
    this.authorInfo = authorInfo;
  }

  /**
   * write permission
   *
   * @param authorReq AuthorReq
   * @return TSStatus
   */
  public TSStatus operatePermission(AuthorReq authorReq) {
    return getConsensusManager().write(authorReq).getStatus();
  }

  /**
   * Query for permissions
   *
   * @param authorReq AuthorReq
   * @return PermissionInfoResp
   */
  public PermissionInfoResp queryPermission(AuthorReq authorReq) {
    return (PermissionInfoResp) getConsensusManager().read(authorReq).getDataset();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  public TSStatus login(String username, String password) {
    return authorInfo.login(username, password);
  }

  public TSStatus checkUserPrivileges(String username, List<String> paths, int permission) {
    return authorInfo.checkUserPrivileges(username, paths, permission);
  }
}
