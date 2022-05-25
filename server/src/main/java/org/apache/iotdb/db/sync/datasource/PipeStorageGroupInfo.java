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
 *
 */

package org.apache.iotdb.db.sync.datasource;

public class PipeStorageGroupInfo {
  private final String storageGroupName;
  private long nextReadIndex;
  // the data (index <= committedIndex) has been committed.
  private long committedIndex;

  public PipeStorageGroupInfo(String sgName, long committedIndex, long nextReadIndex) {
    this.storageGroupName = sgName;
    this.committedIndex = committedIndex;
    this.nextReadIndex = nextReadIndex;
  }

  public void setCommittedIndex(long committedIndex) {
    this.committedIndex = committedIndex;
  }

  public long getCommittedIndex() {
    return committedIndex;
  }

  public void setNextReadIndex(long nextReadIndex) {
    this.nextReadIndex = nextReadIndex;
  }

  public long getNextReadIndex() {
    return nextReadIndex;
  }
}
