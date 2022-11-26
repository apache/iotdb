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
package org.apache.iotdb.commons.sync.transport;

import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;

public class SyncIdentityInfo {
  public String pipeName;
  public long createTime;
  public String version;
  public String remoteAddress;
  public String database;

  public SyncIdentityInfo(TSyncIdentityInfo identityInfo, String remoteAddress) {
    this.pipeName = identityInfo.getPipeName();
    this.createTime = identityInfo.getCreateTime();
    this.version = identityInfo.getVersion();
    this.database = identityInfo.getDatabase();
    this.remoteAddress = remoteAddress;
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreateTime() {
    return createTime;
  }

  public String getVersion() {
    return version;
  }

  public String getRemoteAddress() {
    return remoteAddress;
  }

  public String getDatabase() {
    return database;
  }
}
