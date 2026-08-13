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

package org.apache.iotdb.db.storageengine.rescon.quotas;

public class AcquireContext {

  private String queryId;
  private String fragmentId;
  private String requestId;
  private String statementType;

  public String getQueryId() {
    return queryId;
  }

  public AcquireContext setQueryId(String queryId) {
    this.queryId = queryId;
    return this;
  }

  public String getFragmentId() {
    return fragmentId;
  }

  public AcquireContext setFragmentId(String fragmentId) {
    this.fragmentId = fragmentId;
    return this;
  }

  public String getRequestId() {
    return requestId;
  }

  public AcquireContext setRequestId(String requestId) {
    this.requestId = requestId;
    return this;
  }

  public String getStatementType() {
    return statementType;
  }

  public AcquireContext setStatementType(String statementType) {
    this.statementType = statementType;
    return this;
  }
}
