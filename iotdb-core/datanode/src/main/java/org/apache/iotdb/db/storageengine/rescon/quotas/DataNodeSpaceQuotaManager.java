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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DataNodeSpaceQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeSpaceQuotaManager.class);

  private ConcurrentMap<String, TSpaceQuota> spaceQuotaLimit;
  private ConcurrentMap<String, TSpaceQuota> spaceQuotaUsage;
  private DataNodeSizeStore dataNodeSizeStore;

  public DataNodeSpaceQuotaManager() {
    spaceQuotaLimit = new ConcurrentHashMap<>();
    spaceQuotaUsage = new ConcurrentHashMap<>();
    dataNodeSizeStore = new DataNodeSizeStore();
    recover();
  }

  public DataNodeSpaceQuotaManager(
      final ConcurrentMap<String, TSpaceQuota> spaceQuotaLimit,
      final ConcurrentMap<String, TSpaceQuota> spaceQuotaUsage) {
    this.spaceQuotaLimit = spaceQuotaLimit;
    this.spaceQuotaUsage = spaceQuotaUsage;
  }

  /** SingleTon */
  private static class DataNodeSpaceQuotaManagerHolder {
    private static final DataNodeSpaceQuotaManager INSTANCE = new DataNodeSpaceQuotaManager();

    private DataNodeSpaceQuotaManagerHolder() {}
  }

  public static DataNodeSpaceQuotaManager getInstance() {
    return DataNodeSpaceQuotaManagerHolder.INSTANCE;
  }

  public TSStatus setSpaceQuota(final TSetSpaceQuotaReq req) {
    for (final String database : req.getDatabase()) {
      spaceQuotaUsage.put(database, new TSpaceQuota());
      spaceQuotaLimit.put(database, req.getSpaceLimit());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  private void recover() {
    final TSpaceQuotaResp spaceQuota = ClusterConfigTaskExecutor.getInstance().getSpaceQuota();
    if (spaceQuota.getStatus() != null) {
      if (spaceQuota.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && spaceQuota.getSpaceQuota() != null) {
        for (final String database : spaceQuota.getSpaceQuota().keySet()) {
          spaceQuotaUsage.put(database, new TSpaceQuota());
          spaceQuotaLimit.put(database, spaceQuota.getSpaceQuota().get(database));
        }
      }
      LOGGER.info("Space quota limit restore succeeded, limit: {}.", spaceQuotaLimit);
    } else {
      LOGGER.error("Space quota limit restore failed, limit: {}.", spaceQuotaLimit);
    }
  }

  public boolean checkDeviceLimit(String database) {
    database = IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + database;
    final TSpaceQuota spaceQuota = spaceQuotaLimit.get(database);
    return spaceQuota == null
        || spaceQuota.getDeviceNum() == 0
        || spaceQuota.getDeviceNum() == -1
        || spaceQuota.getDeviceNum() - spaceQuotaUsage.get(database).getDeviceNum() > 0;
  }

  public void updateSpaceQuotaUsage(final Map<String, TSpaceQuota> spaceQuotaUsage) {
    if (Objects.nonNull(spaceQuotaUsage)) {
      this.spaceQuotaUsage.putAll(spaceQuotaUsage);
    }
  }

  public boolean checkTimeSeriesNum(String database) {
    database = IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + database;
    final TSpaceQuota spaceQuota = spaceQuotaLimit.get(database);
    return spaceQuota == null
        || spaceQuota.getTimeserieNum() == 0
        || spaceQuota.getTimeserieNum() == -1
        || spaceQuota.getTimeserieNum() - spaceQuotaUsage.get(database).getTimeserieNum() > 0;
  }

  public boolean checkRegionDisk(String database) {
    TSpaceQuota spaceQuota = spaceQuotaLimit.get(database);
    return spaceQuota == null
        || spaceQuota.getDiskSize() == 0
        || spaceQuota.getDiskSize() == -1
        || spaceQuota.getDiskSize() - spaceQuotaUsage.get(database).getDiskSize() > 0;
  }

  public void setDataRegionIds(final List<Integer> dataRegionIds) {
    dataNodeSizeStore.setDataRegionIds(dataRegionIds);
  }

  public Map<Integer, Long> getRegionDisk() {
    return dataNodeSizeStore.getDataRegionDisk();
  }
}
