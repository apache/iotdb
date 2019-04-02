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
package org.apache.iotdb.cluster.rpc.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class BasicResponse implements Serializable {

  private static final long serialVersionUID = 7509860476962493127L;

  /**
   * Group ID
   */
  private String groupId;
  /**
   * Mark if it needs to redirect to right leader
   */
  private boolean redirected;
  /**
   * Mark if item of a request is success
   */
  private List<Boolean> results;
  /**
   * Redirect leader id
   */
  private String leaderStr;
  /**
   * Error message
   */
  private String errorMsg;

  public BasicResponse(String groupId, boolean redirected, String leaderStr, String errorMsg) {
    this.groupId = groupId;
    this.redirected = redirected;
    this.results = new ArrayList<>();
    this.errorMsg = errorMsg;
    this.leaderStr = leaderStr;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public boolean isRedirected() {
    return redirected;
  }

  public void setRedirected(boolean redirected) {
    this.redirected = redirected;
  }

  public String getLeaderStr() {
    return leaderStr;
  }

  public void setLeaderStr(String leaderStr) {
    this.leaderStr = leaderStr;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }

  public void addResult(boolean success) {
    results.add(success);
  }

  public List<Boolean> getResults() {
    return results;
  }

  public boolean isSuccess() {
    if (errorMsg != null) {
      return false;
    }
    for (boolean result : results) {
      if (!result) {
        return false;
      }
    }
    return true;
  }
}
