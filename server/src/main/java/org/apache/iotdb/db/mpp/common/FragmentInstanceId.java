/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.common;

/** The fragment instance ID class. */
public class FragmentInstanceId {

  private final String fullId;
  private final QueryId queryId;
  private final FragmentId fragmentId;
  private final String instanceId;

  public FragmentInstanceId(QueryId queryId, FragmentId fragmentId, String instanceId) {
    this.queryId = queryId;
    this.fragmentId = fragmentId;
    this.instanceId = instanceId;
    this.fullId = String.format("%s.%d.%s", queryId.getId(), fragmentId.getId(), instanceId);
  }

  public String getFullId() {
    return fullId;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public FragmentId getFragmentId() {
    return fragmentId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String toString() {
    return fullId;
  }
}
