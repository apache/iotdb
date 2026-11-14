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
 * Answer to {@link UpdateSyncReq}: {@code granted} once the new sync number is durable; {@code
 * syncNumber} is the voter's (possibly unchanged) sync number. {@code contiguousEnd} is optional
 * ({@link #UNSET} if absent).
 */
public final class UpdateSyncRes extends Message {

  public static final long UNSET = Long.MIN_VALUE;

  public final boolean granted;
  public final long syncNumber;
  public final long contiguousEnd;

  public UpdateSyncRes(long term, boolean granted, long syncNumber, long contiguousEnd) {
    super(term);
    this.granted = granted;
    this.syncNumber = syncNumber;
    this.contiguousEnd = contiguousEnd;
  }

  public boolean hasContiguousEnd() {
    return contiguousEnd != UNSET;
  }

  @Override
  public String kind() {
    return "USR";
  }

  @Override
  public String brief() {
    return "UpdateSyncResp("
        + (granted ? "GRANT" : "REJECT")
        + ",sync="
        + syncNumber
        + (hasContiguousEnd() ? ",ce=" + contiguousEnd : "")
        + ") t="
        + term;
  }
}
