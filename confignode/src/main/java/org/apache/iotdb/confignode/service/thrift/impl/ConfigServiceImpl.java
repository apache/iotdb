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
package org.apache.iotdb.confignode.service.thrift.impl;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.service.basic.ConfigServiceProvider;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;

import java.util.List;

/** ConfigServiceImpl exposes the interface that interacts with the DataNode */
public class ConfigServiceImpl implements ConfigIService.Iface {

  private ConfigServiceProvider configServiceProvider;
  private ConfigManager configManager;

  public ConfigServiceImpl() throws QueryProcessException {
    this.configServiceProvider = new ConfigServiceProvider(null);
    this.configManager = new ConfigManager();
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteStorageGroup(long sessionId, List<String> storageGroups) throws TException {
    return null;
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createAlignedTimeseries(TSCreateAlignedTimeseriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createMultiTimeseries(TSCreateMultiTimeseriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteTimeSeries(long sessionId, List<String> paths) throws TException {
    return null;
  }

  @Override
  public int getDeviceGroupID(long sessionId, String device) throws TException {
    return -1;
  }

  // TODO: Interfaces for data operations
}
