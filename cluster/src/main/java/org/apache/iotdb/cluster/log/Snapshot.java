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

package org.apache.iotdb.cluster.log;


import java.nio.ByteBuffer;

/**
 * As we can only hold a certain amount of logs in memory, when the logs' size exceed the memory
 * capacity, they will be deleted or compressed, and a snapshot of current system will be
 * generated. If a node need to catch up but its next log is deleted, it will catch up based on
 * the latest snapshot and the logs still in memory.
 * The snapshot could be a file recording the list of current system files, or the compressed all
 * historical logs, depending on the implementation.
 */
//TODO-CLuster: implement the Snapshot mechanism
public abstract class Snapshot {

  long lastLogId;
  long lastLogTerm;

  public abstract ByteBuffer serialize();

  public abstract void deserialize(ByteBuffer buffer);

  public void setLastLogId(long lastLogId) {
    this.lastLogId = lastLogId;
  }

  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }

  public long getLastLogId() {
    return lastLogId;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }
}
