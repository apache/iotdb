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

public abstract class BasicResponse implements Serializable {

  private static final long serialVersionUID = 7509860476962493127L;
  /**
   * Mark if it needs to redirect to right leader
   */
  private boolean redirected;
  /**
   * Mark if the request is success
   */
  private boolean success;
  /**
   * Redirect leader id
   */
  private String leaderStr;
  /**
   * Error message
   */
  private String errorMsg;

  public BasicResponse(boolean redirected, boolean success, String leaderStr, String errorMsg) {
    this.redirected = redirected;
    this.success = success;
    this.errorMsg = errorMsg;
    this.leaderStr = leaderStr;
  }

  public boolean isRedirected() {
    return redirected;
  }

  public void setRedirected(boolean redirected) {
    this.redirected = redirected;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
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
}
