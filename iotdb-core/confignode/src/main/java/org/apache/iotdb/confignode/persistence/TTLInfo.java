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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
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
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TTLInfo implements SnapshotProcessor {
  private static final String SNAPSHOT_FILENAME = "ttl_info.bin";
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
      ttlCache.setTTL(plan.getPathPattern(), plan.getTTL());
      if (plan.isDataBase()) {
        // set ttl to path.**
        ttlCache.setTTL(
            new PartialPath(plan.getPathPattern())
                .concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)
                .getNodes(),
            plan.getTTL());
      }
    } finally {
      lock.writeLock().unlock();
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus unsetTTL(SetTTLPlan plan) {
    lock.writeLock().lock();
    try {
      ttlCache.unsetTTL(plan.getPathPattern());
      if (plan.isDataBase()) {
        // unset ttl to path.**
        ttlCache.unsetTTL(
            new PartialPath(plan.getPathPattern())
                .concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)
                .getNodes());
      }
    } finally {
      lock.writeLock().unlock();
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public ShowTTLResp showAllTTL() {
    lock.readLock().lock();
    ShowTTLResp resp = new ShowTTLResp();
    try {
      Map<String, Long> pathTTLMap = ttlCache.getAllPathTTL();
      resp.setPathTTLMap(pathTTLMap);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } finally {
      lock.readLock().unlock();
    }
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
        && this.showAllTTL().getPathTTLMap().equals(other.showAllTTL().getPathTTLMap());
  }
}
