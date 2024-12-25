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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.List;
import java.util.Optional;

public class FragmentInstanceInfo implements DataSet {
  private final FragmentInstanceState state;
  private String message;
  private long endTime;

  private List<FragmentInstanceFailureInfo> failureInfoList;

  private TSStatus errorCode;

  public FragmentInstanceInfo(FragmentInstanceState state) {
    this.state = state;
  }

  public FragmentInstanceInfo(FragmentInstanceState state, long endTime) {
    this.state = state;
    this.endTime = endTime;
  }

  public FragmentInstanceInfo(
      FragmentInstanceState state,
      long endTime,
      String message,
      List<FragmentInstanceFailureInfo> failureInfoList) {
    this(state, endTime);
    this.message = message;
    this.failureInfoList = failureInfoList;
  }

  public FragmentInstanceInfo(
      FragmentInstanceState state,
      long endTime,
      String message,
      List<FragmentInstanceFailureInfo> failureInfoList,
      TSStatus errorStatus) {
    this(state, endTime);
    this.message = message;
    this.failureInfoList = failureInfoList;
    this.errorCode = errorStatus;
  }

  public FragmentInstanceState getState() {
    return state;
  }

  public long getEndTime() {
    return endTime;
  }

  public String getMessage() {
    return message;
  }

  public Optional<TSStatus> getErrorCode() {
    return Optional.ofNullable(errorCode);
  }

  public List<FragmentInstanceFailureInfo> getFailureInfoList() {
    return failureInfoList;
  }
}
