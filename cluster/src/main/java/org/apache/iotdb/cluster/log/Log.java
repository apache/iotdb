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
 * Log records operations that are made on this cluster. Each log records 4 longs currLogIndex
 * (may be replaced by previousLogIndex + 1), currLogTerm, previousLogIndex, previousLogTerm, so
 * that the logs in a cluster will form a log chain and abnormal operations can thus be
 * distinguished and removed.
 */
public abstract class Log {

  private long currLogIndex;
  private long currLogTerm;
  private long previousLogIndex;
  private long previousLogTerm;

  public abstract ByteBuffer serialize();

  public abstract void deserialize(ByteBuffer buffer);

  public enum Types {
    // TODO-Cluster support more logs
    ADD_NODE
  }

  public long getPreviousLogIndex() {
    return previousLogIndex;
  }

  public void setPreviousLogIndex(long previousLogIndex) {
    this.previousLogIndex = previousLogIndex;
  }

  public long getPreviousLogTerm() {
    return previousLogTerm;
  }

  public void setPreviousLogTerm(long previousLogTerm) {
    this.previousLogTerm = previousLogTerm;
  }

  public long getCurrLogIndex() {
    return currLogIndex;
  }

  public void setCurrLogIndex(long currLogIndex) {
    this.currLogIndex = currLogIndex;
  }

  public long getCurrLogTerm() {
    return currLogTerm;
  }

  public void setCurrLogTerm(long currLogTerm) {
    this.currLogTerm = currLogTerm;
  }
}
