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

package org.apache.iotdb.confignode.persistence.quota;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QuotaInfo implements SnapshotProcessor {

  private static final Logger logger = LoggerFactory.getLogger(QuotaInfo.class);

  private final ReentrantReadWriteLock spaceQuotaReadWriteLock;
  private final Map<String, TSpaceQuota> spaceQuotaLimit;
  private final Map<String, TSpaceQuota> spaceQuotaUsage;
  private final Map<String, TThrottleQuota> throttleQuotaLimit;

  private final String snapshotFileName = "quota_info.bin";

  public QuotaInfo() {
    spaceQuotaReadWriteLock = new ReentrantReadWriteLock();
    spaceQuotaLimit = new HashMap<>();
    spaceQuotaUsage = new HashMap<>();
    throttleQuotaLimit = new HashMap<>();
  }

  public TSStatus setSpaceQuota(SetSpaceQuotaPlan setSpaceQuotaPlan) {
    for (String database : setSpaceQuotaPlan.getPrefixPathList()) {
      TSpaceQuota spaceQuota = setSpaceQuotaPlan.getSpaceLimit();
      // “0” means that the user has not reset the value of the space quota type
      // So the old values are still used
      if (spaceQuotaLimit.containsKey(database)) {
        if (spaceQuota.getDeviceNum() == 0) {
          spaceQuota.setDeviceNum(spaceQuotaLimit.get(database).getDeviceNum());
        }
        if (spaceQuota.getTimeserieNum() == 0) {
          spaceQuota.setTimeserieNum(spaceQuotaLimit.get(database).getTimeserieNum());
        }
        if (spaceQuota.getDiskSize() == 0) {
          spaceQuota.setDiskSize(spaceQuotaLimit.get(database).getDiskSize());
        }
        if (spaceQuota.getDeviceNum() == -1) {
          spaceQuota.setDeviceNum(0);
        }
        if (spaceQuota.getTimeserieNum() == -1) {
          spaceQuota.setTimeserieNum(0);
        }
        if (spaceQuota.getDiskSize() == -1) {
          spaceQuota.setDiskSize(0);
        }
      }
      if (!spaceQuotaUsage.containsKey(database)) {
        spaceQuotaUsage.put(database, new TSpaceQuota());
      }
      spaceQuotaLimit.put(database, spaceQuota);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus setThrottleQuota(SetThrottleQuotaPlan setThrottleQuotaPlan) {
    TThrottleQuota throttleQuota = setThrottleQuotaPlan.getThrottleQuota();
    String userName = setThrottleQuotaPlan.getUserName();
    if (throttleQuotaLimit.containsKey(setThrottleQuotaPlan.getUserName())) {
      // about memory
      if (setThrottleQuotaPlan.getThrottleQuota().getMemLimit() == -1) {
        throttleQuotaLimit.get(userName).setMemLimit(0);
      } else if (setThrottleQuotaPlan.getThrottleQuota().getMemLimit() != 0) {
        throttleQuotaLimit.get(userName).setMemLimit(throttleQuota.getMemLimit());
      }

      // about cpu
      if (setThrottleQuotaPlan.getThrottleQuota().getCpuLimit() == -1) {
        throttleQuotaLimit.get(userName).setCpuLimit(0);
      } else if (setThrottleQuotaPlan.getThrottleQuota().getCpuLimit() != 0) {
        throttleQuotaLimit.get(userName).setCpuLimit(throttleQuota.getCpuLimit());
      }
      if (!throttleQuota.getThrottleLimit().isEmpty()) {
        for (ThrottleType throttleType : throttleQuota.getThrottleLimit().keySet()) {
          if (throttleQuotaLimit.get(userName).getThrottleLimit().containsKey(throttleType)) {
            throttleQuotaLimit
                .get(userName)
                .getThrottleLimit()
                .get(throttleType)
                .setSoftLimit(throttleQuota.getThrottleLimit().get(throttleType).getSoftLimit());
            throttleQuotaLimit
                .get(userName)
                .getThrottleLimit()
                .get(throttleType)
                .setTimeUnit(throttleQuota.getThrottleLimit().get(throttleType).getTimeUnit());
          } else {
            throttleQuotaLimit
                .get(userName)
                .getThrottleLimit()
                .put(throttleType, throttleQuota.getThrottleLimit().get(throttleType));
          }
        }
      }
    } else {
      throttleQuotaLimit.put(
          setThrottleQuotaPlan.getUserName(), setThrottleQuotaPlan.getThrottleQuota());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public Map<String, TSpaceQuota> getSpaceQuotaLimit() {
    return spaceQuotaLimit;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      logger.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    spaceQuotaReadWriteLock.writeLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      serializeSpaceQuotaLimit(fileOutputStream);
      serializeThrottleQuotaLimit(fileOutputStream);
    } finally {
      spaceQuotaReadWriteLock.writeLock().unlock();
    }
    return true;
  }

  private void serializeSpaceQuotaLimit(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(spaceQuotaLimit.size(), fileOutputStream);
    for (Map.Entry<String, TSpaceQuota> spaceQuotaEntry : spaceQuotaLimit.entrySet()) {
      ReadWriteIOUtils.write(spaceQuotaEntry.getKey(), fileOutputStream);
      ReadWriteIOUtils.write(spaceQuotaEntry.getValue().getDeviceNum(), fileOutputStream);
      ReadWriteIOUtils.write(spaceQuotaEntry.getValue().getTimeserieNum(), fileOutputStream);
      ReadWriteIOUtils.write(spaceQuotaEntry.getValue().getDiskSize(), fileOutputStream);
    }
  }

  private void serializeThrottleQuotaLimit(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(throttleQuotaLimit.size(), fileOutputStream);
    for (Map.Entry<String, TThrottleQuota> throttleQuotaEntry : throttleQuotaLimit.entrySet()) {
      ReadWriteIOUtils.write(throttleQuotaEntry.getKey(), fileOutputStream);
      TThrottleQuota throttleQuota = throttleQuotaEntry.getValue();
      ReadWriteIOUtils.write(throttleQuota.getThrottleLimit().size(), fileOutputStream);
      for (Map.Entry<ThrottleType, TTimedQuota> entry :
          throttleQuota.getThrottleLimit().entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().name(), fileOutputStream);
        ReadWriteIOUtils.write(entry.getValue().getTimeUnit(), fileOutputStream);
        ReadWriteIOUtils.write(entry.getValue().getSoftLimit(), fileOutputStream);
      }
      ReadWriteIOUtils.write(throttleQuota.getMemLimit(), fileOutputStream);
      ReadWriteIOUtils.write(throttleQuota.getCpuLimit(), fileOutputStream);
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      logger.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    spaceQuotaReadWriteLock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      clear();
      deserializeSpaceQuotaLimit(fileInputStream);
      deserializeThrottleQuotaLimit(fileInputStream);
    } finally {
      spaceQuotaReadWriteLock.writeLock().unlock();
    }
  }

  private void deserializeSpaceQuotaLimit(FileInputStream fileInputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(fileInputStream);
    while (size > 0) {
      String path = ReadWriteIOUtils.readString(fileInputStream);
      TSpaceQuota spaceQuota = new TSpaceQuota();
      spaceQuota.setDeviceNum(ReadWriteIOUtils.readLong(fileInputStream));
      spaceQuota.setTimeserieNum(ReadWriteIOUtils.readLong(fileInputStream));
      spaceQuota.setDiskSize(ReadWriteIOUtils.readLong(fileInputStream));
      spaceQuotaLimit.put(path, spaceQuota);
      size--;
    }
  }

  private void deserializeThrottleQuotaLimit(FileInputStream fileInputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(fileInputStream);
    while (size > 0) {
      String userName = ReadWriteIOUtils.readString(fileInputStream);
      int quotaSize = ReadWriteIOUtils.readInt(fileInputStream);
      Map<ThrottleType, TTimedQuota> quotaLimit = new HashMap<>();
      TThrottleQuota throttleQuota = new TThrottleQuota();
      while (quotaSize > 0) {
        ThrottleType throttleType =
            ThrottleType.valueOf(ReadWriteIOUtils.readString(fileInputStream));
        long timeUnit = ReadWriteIOUtils.readLong(fileInputStream);
        long softLimit = ReadWriteIOUtils.readLong(fileInputStream);
        quotaLimit.put(throttleType, new TTimedQuota(timeUnit, softLimit));
        quotaSize--;
      }
      throttleQuota.setThrottleLimit(quotaLimit);
      throttleQuota.setMemLimit(ReadWriteIOUtils.readLong(fileInputStream));
      throttleQuota.setCpuLimit(ReadWriteIOUtils.readInt(fileInputStream));
      throttleQuotaLimit.put(userName, throttleQuota);
      size--;
    }
  }

  public Map<String, TSpaceQuota> getSpaceQuotaUsage() {
    return spaceQuotaUsage;
  }

  public Map<String, TThrottleQuota> getThrottleQuotaLimit() {
    return throttleQuotaLimit;
  }

  public void clear() {
    spaceQuotaLimit.clear();
    throttleQuotaLimit.clear();
  }
}
