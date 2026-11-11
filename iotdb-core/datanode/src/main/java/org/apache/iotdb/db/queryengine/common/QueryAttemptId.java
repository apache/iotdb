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
package org.apache.iotdb.db.queryengine.common;

/** Identifies one execution of a query, independently of transport-level RPC retries. */
public final class QueryAttemptId {
  public static final int LEGACY_ATTEMPT = -1;

  private final QueryId queryId;
  private final int attemptId;

  public QueryAttemptId(QueryId queryId, int attemptId) {
    this.queryId = queryId;
    this.attemptId = attemptId;
  }

  public boolean isLegacy() {
    return attemptId == LEGACY_ATTEMPT;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof QueryAttemptId)) {
      return false;
    }
    QueryAttemptId that = (QueryAttemptId) other;
    return attemptId == that.attemptId && queryId.equals(that.queryId);
  }

  @Override
  public int hashCode() {
    return 31 * queryId.hashCode() + attemptId;
  }

  @Override
  public String toString() {
    return queryId + "/" + attemptId;
  }
}
