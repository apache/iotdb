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

package org.apache.iotdb.consensus.raft.core.msg;

/**
 * ParallelRaft term recovery, collection step: a recovering leader (term {@code term}, sync number
 * {@code syncTerm}) asks a voter for its promise and its entries of term {@code syncTerm} from
 * {@code fromIndex} on, at most {@code maxEntries} entries / {@code maxBytes} payload bytes per
 * page.
 */
public final class RecoverTermReq extends Message {

  public final int leaderId;
  public final long syncTerm;
  public final long fromIndex;
  public final int maxEntries;
  public final long maxBytes;

  public RecoverTermReq(
      long term, int leaderId, long syncTerm, long fromIndex, int maxEntries, long maxBytes) {
    super(term);
    this.leaderId = leaderId;
    this.syncTerm = syncTerm;
    this.fromIndex = fromIndex;
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
  }

  @Override
  public String kind() {
    return "RT";
  }

  @Override
  public long matchIndex() {
    return fromIndex;
  }

  @Override
  public String brief() {
    return "RecoverTerm(t="
        + term
        + ",sync="
        + syncTerm
        + ",from="
        + fromIndex
        + ",max="
        + maxEntries
        + "/"
        + maxBytes
        + "B) from N"
        + leaderId;
  }
}
