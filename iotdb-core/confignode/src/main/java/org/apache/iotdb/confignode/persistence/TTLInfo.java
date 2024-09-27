/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.read.ttl.ShowTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.response.ttl.ShowTTLResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TTLInfo implements SnapshotProcessor {
  public static final String SNAPSHOT_FILENAME = "ttl_info.bin";
  private static final Logger LOGGER = LoggerFactory.getLogger(TTLInfo.class);

  private final TTLCache ttlCache;

  private final ReadWriteLock lock;

  public TTLInfo() {
    ttlCache = new TTLCache();
    lock = new ReentrantReadWriteLock();
  }

  public TSStatus setTTL(SetTTLPlan plan) {
    lock.writeLock().lock();
    try {
      // check ttl rule capacity
      final int tTlRuleCapacity = CommonDescriptor.getInstance().getConfig().getTTlRuleCapacity();
      if (getTTLCount() >= tTlRuleCapacity) {
        TSStatus errorStatus = new TSStatus(TSStatusCode.OVERSIZE_TTL.getStatusCode());
        errorStatus.setMessage(
            String.format(
                "The number of TTL rules has reached the limit (%d). Please delete "
                    + "some existing rules first.",
                tTlRuleCapacity));
        return errorStatus;
      }
      ttlCache.setTTL(plan.getPathPattern(), plan.getTTL());
      if (plan.isDataBase()) {
        // set ttl to path.**
        String[] pathNodes = Arrays.copyOf(plan.getPathPattern(), plan.getPathPattern().length + 1);
        pathNodes[pathNodes.length - 1] = IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
        ttlCache.setTTL(pathNodes, plan.getTTL());
      }
    } finally {
      lock.writeLock().unlock();
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /** Only used for upgrading from database level ttl to device level ttl. */
  public void setTTL(Map<String, Long> databaseTTLMap) throws IllegalPathException {
    lock.writeLock().lock();
    try {
      for (Map.Entry<String, Long> entry : databaseTTLMap.entrySet()) {
        String[] nodesWithWildcard =
            PathUtils.splitPathToDetachedNodes(
                entry
                    .getKey()
                    .concat(
                        IoTDBConstant.PATH_SEPARATOR + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
        ttlCache.setTTL(nodesWithWildcard, entry.getValue());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus unsetTTL(SetTTLPlan plan) {
    TSStatus status;
    lock.writeLock().lock();
    try {
      status = ttlCache.unsetTTL(plan.getPathPattern());
      if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode() && plan.isDataBase()) {
        // unset ttl to path.**
        status =
            ttlCache.unsetTTL(
                new PartialPath(plan.getPathPattern())
                    .concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)
                    .getNodes());
      }
    } finally {
      lock.writeLock().unlock();
    }
    return status;
  }

  public ShowTTLResp showTTL(ShowTTLPlan plan) {
    ShowTTLResp resp = new ShowTTLResp();
    Map<String, Long> pathTTLMap = new HashMap<>();
    lock.readLock().lock();
    try {
      PartialPath pathPattern = new PartialPath(plan.getPathPattern());
      for (Map.Entry<String[], Long> entry : ttlCache.getAllTTLs().entrySet()) {
        if (pathPattern.matchFullPath(entry.getKey())) {
          pathTTLMap.put(
              String.join(String.valueOf(IoTDBConstant.PATH_SEPARATOR), entry.getKey()),
              entry.getValue());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    resp.setPathTTLMap(pathTTLMap);
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return resp;
  }

  public int getTTLCount() {
    lock.readLock().lock();
    try {
      return ttlCache.getTtlCount();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot of TTLInfo, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }
    lock.writeLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      ttlCache.serialize(outputStream);
      fileOutputStream.getFD().sync();
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot of TTLInfo, snapshot file [{}] does not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    lock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      ttlCache.clear();
      ttlCache.deserialize(bufferedInputStream);
    } catch (IllegalPathException e) {
      throw new IOException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TTLInfo other = (TTLInfo) o;
    return this.getTTLCount() == other.getTTLCount()
        && this.showTTL(new ShowTTLPlan())
            .getPathTTLMap()
            .equals(other.showTTL(new ShowTTLPlan()).getPathTTLMap());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTTLCount(), showTTL(new ShowTTLPlan()).getPathTTLMap());
  }

  @TestOnly
  public void clear() {
    ttlCache.clear();
  }
}
