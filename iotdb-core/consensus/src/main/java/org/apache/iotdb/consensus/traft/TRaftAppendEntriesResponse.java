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

package org.apache.iotdb.consensus.traft;

/** Reply to an AppendEntries attempt, including a hint for the next retry index. */
class TRaftAppendEntriesResponse {

  private final boolean success;
  private final long term;
  private final long matchIndex;
  private final long nextIndexHint;

  TRaftAppendEntriesResponse(boolean success, long term, long matchIndex, long nextIndexHint) {
    this.success = success;
    this.term = term;
    this.matchIndex = matchIndex;
    this.nextIndexHint = nextIndexHint;
  }

  boolean isSuccess() {
    return success;
  }

  long getTerm() {
    return term;
  }

  long getMatchIndex() {
    return matchIndex;
  }

  long getNextIndexHint() {
    return nextIndexHint;
  }
}
