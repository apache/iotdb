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

package org.apache.iotdb.confignode.consensus.response.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TListUserInfo;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.List;

public class PermissionInfoResp implements DataSet {

  private TSStatus status;

  private String tag;
  private List<String> memberList;

  private List<TListUserInfo> usersInfo;

  private TPermissionInfoResp permissionInfoResp;

  public PermissionInfoResp() {}

  public PermissionInfoResp(TSStatus status, String tag, List<String> info) {
    this.status = status;
    this.tag = tag;
    this.memberList = info;
  }

  public PermissionInfoResp(TSStatus status) {
    this.status = status;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getTag() {
    return tag;
  }

  public void setMemberInfo(List<String> info) {
    this.memberList = info;
  }

  public List<String> getMemberList() {
    return memberList;
  }

  public void setUsersInfo(List<TListUserInfo> usersInfo) {
    this.usersInfo = usersInfo;
  }

  public List<TListUserInfo> getUsersInfo() {
    return usersInfo;
  }

  public TPermissionInfoResp getPermissionInfoResp() {
    return permissionInfoResp;
  }

  public void setPermissionInfoResp(TPermissionInfoResp resp) {
    this.permissionInfoResp = resp;
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }
}
