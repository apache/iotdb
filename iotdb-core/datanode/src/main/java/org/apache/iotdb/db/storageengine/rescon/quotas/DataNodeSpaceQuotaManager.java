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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataNodeSpaceQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeSpaceQuotaManager.class);

  private Map<String, TSpaceQuota> spaceQuotaLimit;
  private Map<String, TSpaceQuota> spaceQuotaUsage;
  private DataNodeSizeStore dataNodeSizeStore;

  public DataNodeSpaceQuotaManager() {
    spaceQuotaLimit = new HashMap<>();
    spaceQuotaUsage = new HashMap<>();
    dataNodeSizeStore = new DataNodeSizeStore();
    recover();
  }

  public DataNodeSpaceQuotaManager(
      Map<String, TSpaceQuota> spaceQuotaLimit, Map<String, TSpaceQuota> spaceQuotaUsage) {
    this.spaceQuotaLimit = spaceQuotaLimit;
    this.spaceQuotaUsage = spaceQuotaUsage;
  }

  /** SingleTone */
  private static class DataNodeSpaceQuotaManagerHolder {
    private static final DataNodeSpaceQuotaManager INSTANCE = new DataNodeSpaceQuotaManager();

    private DataNodeSpaceQuotaManagerHolder() {}
  }

  public static DataNodeSpaceQuotaManager getInstance() {
    return DataNodeSpaceQuotaManagerHolder.INSTANCE;
  }

  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) {
    for (String database : req.getDatabase()) {
      spaceQuotaLimit.put(database, req.getSpaceLimit());
      spaceQuotaUsage.put(database, new TSpaceQuota());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  private void recover() {
    TSpaceQuotaResp spaceQuota = ClusterConfigTaskExecutor.getInstance().getSpaceQuota();
    if (spaceQuota.getStatus() != null) {
      if (spaceQuota.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && spaceQuota.getSpaceQuota() != null) {
        for (String database : spaceQuota.getSpaceQuota().keySet()) {
          spaceQuotaLimit.put(database, spaceQuota.getSpaceQuota().get(database));
          spaceQuotaUsage.put(database, new TSpaceQuota());
        }
      }
      LOGGER.info("Space quota limit restored succeeded. " + spaceQuotaLimit.toString());
    } else {
      LOGGER.error("Space quota limit restored failed. " + spaceQuotaLimit.toString());
    }
  }

  public boolean checkDeviceLimit(String database) {
    database = IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + database;
    TSpaceQuota spaceQuota = spaceQuotaLimit.get(database);
    if (spaceQuota == null) {
      return true;
    } else if (spaceQuota.getDeviceNum() == 0 || spaceQuota.getDeviceNum() == -1) {
      return true;
    }
    long deviceNum = spaceQuotaUsage.get(database).getDeviceNum();
    if (spaceQuota.getDeviceNum() - deviceNum > 0) {
      return true;
    }
    return false;
  }

  public void updateSpaceQuotaUsage(Map<String, TSpaceQuota> spaceQuotaUsage) {
    this.spaceQuotaUsage = spaceQuotaUsage;
  }

  public boolean checkTimeSeriesNum(String database) {
    database = IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + database;
    TSpaceQuota spaceQuota = spaceQuotaLimit.get(database);
    if (spaceQuota == null) {
      return true;
    } else if (spaceQuota.getTimeserieNum() == 0 || spaceQuota.getTimeserieNum() == -1) {
      return true;
    }
    long timeSeriesNum = spaceQuotaUsage.get(database).getTimeserieNum();
    if (spaceQuota.getTimeserieNum() - timeSeriesNum > 0) {
      return true;
    }
    return false;
  }

  public boolean checkRegionDisk(String database) {
    TSpaceQuota spaceQuota = spaceQuotaLimit.get(database);
    if (spaceQuota == null) {
      return true;
    } else if (spaceQuota.getDiskSize() == 0 || spaceQuota.getDiskSize() == -1) {
      return true;
    }
    long diskSize = spaceQuotaUsage.get(database).getDiskSize();
    if (spaceQuota.getDiskSize() - diskSize > 0) {
      return true;
    }
    return false;
  }

  public void setDataRegionIds(List<Integer> dataRegionIds) {
    dataNodeSizeStore.setDataRegionIds(dataRegionIds);
  }

  public Map<Integer, Long> getRegionDisk() {
    return dataNodeSizeStore.getDataRegionDisk();
  }

  public void setSpaceQuotaLimit(Map<String, TSpaceQuota> spaceQuotaLimit) {
    this.spaceQuotaLimit = spaceQuotaLimit;
  }
}
