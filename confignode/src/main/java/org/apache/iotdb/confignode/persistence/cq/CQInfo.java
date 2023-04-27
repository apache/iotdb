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
package org.apache.iotdb.confignode.persistence.cq;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cq.CQState;
import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.consensus.response.cq.ShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@ThreadSafe
public class CQInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CQInfo.class);

  private static final String SNAPSHOT_FILENAME = "cq_info.snapshot";

  private final Map<String, CQEntry> cqMap;

  private final ReadWriteLock lock;

  public CQInfo() {
    this.cqMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Add a new CQ only if there was no mapping for <tt>this cqId</tt>, otherwise ignore this
   * operation.
   *
   * @return SUCCESS_STATUS if there was no mapping for <tt>this cqId</tt>, otherwise
   *     CQ_AlREADY_EXIST
   */
  public TSStatus addCQ(AddCQPlan plan) {
    TSStatus res = new TSStatus();
    String cqId = plan.getReq().cqId;
    lock.writeLock().lock();
    try {
      if (cqMap.containsKey(cqId)) {
        res.code = TSStatusCode.CQ_AlREADY_EXIST.getStatusCode();
        res.message = String.format("CQ %s has already been created.", cqId);
      } else {
        CQEntry cqEntry =
            new CQEntry(
                plan.getReq(),
                plan.getMd5(),
                plan.getFirstExecutionTime() - plan.getReq().everyInterval);
        cqMap.put(cqId, cqEntry);
        res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
      }
      return res;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Drop the CQ whose ID is same as <tt>cqId</tt> in plan.
   *
   * @return SUCCESS_STATUS if there is CQ whose ID and md5 is same as <tt>cqId</tt> in plan,
   *     otherwise NO_SUCH_CQ.
   */
  public TSStatus dropCQ(DropCQPlan plan) {
    TSStatus res = new TSStatus();
    String cqId = plan.getCqId();
    Optional<String> md5 = plan.getMd5();
    lock.writeLock().lock();
    try {
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format("CQ %s doesn't exist.", cqId);
        LOGGER.warn("Drop CQ {} failed, because it doesn't exist.", cqId);
      } else if ((md5.isPresent() && !md5.get().equals(cqEntry.md5))) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format("MD5 of CQ %s doesn't match", cqId);
        LOGGER.warn("Drop CQ {} failed, because its MD5 doesn't match.", cqId);
      } else {
        cqMap.remove(cqId);
        res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
        LOGGER.info("Drop CQ {} successfully.", cqId);
      }
      return res;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ShowCQResp showCQ(ShowCQPlan plan) {
    lock.readLock().lock();
    try {
      return new ShowCQResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          cqMap.values().stream().map(CQEntry::new).collect(Collectors.toList()));
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Change the state of cq to ACTIVE.
   *
   * @return Optional.empty() if there is no such cq, otherwise previous state of this cq.
   */
  public TSStatus activeCQ(ActiveCQPlan plan) {
    TSStatus res = new TSStatus();
    String cqId = plan.getCqId();
    String md5 = plan.getMd5();
    lock.writeLock().lock();
    try {
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format("CQ %s doesn't exist.", cqId);
      } else if (!md5.equals(cqEntry.md5)) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format("MD5 of CQ %s doesn't match", cqId);
      } else if (cqEntry.state == CQState.ACTIVE) {
        res.code = TSStatusCode.CQ_ALREADY_ACTIVE.getStatusCode();
        res.message = String.format("CQ %s has already been active", cqId);
      } else {
        cqEntry.state = CQState.ACTIVE;
        res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
      }
      return res;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Update the last execution time of the corresponding CQ.
   *
   * @return SUCCESS_STATUS if successfully updated, or NO_SUCH_CQ if 1. the CQ doesn't exist; or 2.
   *     md5 is different. or CQ_UPDATE_LAST_EXEC_TIME_FAILED 3. original lastExecutionTime >=
   *     current lastExecutionTime;
   */
  public TSStatus updateCQLastExecutionTime(UpdateCQLastExecTimePlan plan) {
    TSStatus res = new TSStatus();
    String cqId = plan.getCqId();
    String md5 = plan.getMd5();
    lock.writeLock().lock();
    try {
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format("CQ %s doesn't exist.", cqId);
      } else if (!md5.equals(cqEntry.md5)) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format("MD5 of CQ %s doesn't match", cqId);
      } else if (cqEntry.lastExecutionTime >= plan.getExecutionTime()) {
        res.code = TSStatusCode.CQ_UPDATE_LAST_EXEC_TIME_ERROR.getStatusCode();
        res.message =
            String.format(
                "Update last execution time of CQ %s failed because its original last execution time(%d) is larger than the updated one(%d).",
                cqId, cqEntry.lastExecutionTime, plan.getExecutionTime());
      } else {
        cqEntry.lastExecutionTime = plan.getExecutionTime();
        res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
      }
      return res;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot of CQInfo, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    lock.readLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serialize(fileOutputStream);
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(cqMap.size(), stream);
    for (CQEntry entry : cqMap.values()) {
      entry.serialize(stream);
    }
  }

  private void deserialize(InputStream stream) throws IOException {
    int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      CQEntry cqEntry = CQEntry.deserialize(stream);
      cqMap.put(cqEntry.cqId, cqEntry);
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot of CQInfo, snapshot file [{}] does not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    lock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserialize(fileInputStream);

    } finally {
      lock.writeLock().unlock();
    }
  }

  private void clear() {
    cqMap.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CQInfo cqInfo = (CQInfo) o;
    return Objects.equals(cqMap, cqInfo.cqMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cqMap);
  }

  public static class CQEntry {
    private final String cqId;
    private final long everyInterval;
    private final long boundaryTime;
    private final long startTimeOffset;
    private final long endTimeOffset;
    private final TimeoutPolicy timeoutPolicy;
    private final String queryBody;
    private final String sql;
    private final String md5;

    private final String zoneId;

    private final String username;

    private CQState state;
    private long lastExecutionTime;

    private CQEntry(TCreateCQReq req, String md5, long lastExecutionTime) {
      this(
          req.cqId,
          req.everyInterval,
          req.boundaryTime,
          req.startTimeOffset,
          req.endTimeOffset,
          TimeoutPolicy.deserialize(req.timeoutPolicy),
          req.queryBody,
          req.sql,
          md5,
          req.zoneId,
          req.username,
          CQState.INACTIVE,
          lastExecutionTime);
    }

    private CQEntry(CQEntry other) {
      this(
          other.cqId,
          other.everyInterval,
          other.boundaryTime,
          other.startTimeOffset,
          other.endTimeOffset,
          other.timeoutPolicy,
          other.queryBody,
          other.sql,
          other.md5,
          other.zoneId,
          other.username,
          other.state,
          other.lastExecutionTime);
    }

    private CQEntry(
        String cqId,
        long everyInterval,
        long boundaryTime,
        long startTimeOffset,
        long endTimeOffset,
        TimeoutPolicy timeoutPolicy,
        String queryBody,
        String sql,
        String md5,
        String zoneId,
        String username,
        CQState state,
        long lastExecutionTime) {
      this.cqId = cqId;
      this.everyInterval = everyInterval;
      this.boundaryTime = boundaryTime;
      this.startTimeOffset = startTimeOffset;
      this.endTimeOffset = endTimeOffset;
      this.timeoutPolicy = timeoutPolicy;
      this.queryBody = queryBody;
      this.sql = sql;
      this.md5 = md5;
      this.zoneId = zoneId;
      this.username = username;
      this.state = state;
      this.lastExecutionTime = lastExecutionTime;
    }

    private void serialize(OutputStream stream) throws IOException {
      ReadWriteIOUtils.write(cqId, stream);
      ReadWriteIOUtils.write(everyInterval, stream);
      ReadWriteIOUtils.write(boundaryTime, stream);
      ReadWriteIOUtils.write(startTimeOffset, stream);
      ReadWriteIOUtils.write(endTimeOffset, stream);
      ReadWriteIOUtils.write(timeoutPolicy.getType(), stream);
      ReadWriteIOUtils.write(queryBody, stream);
      ReadWriteIOUtils.write(sql, stream);
      ReadWriteIOUtils.write(md5, stream);
      ReadWriteIOUtils.write(zoneId, stream);
      ReadWriteIOUtils.write(username, stream);
      ReadWriteIOUtils.write(state.getType(), stream);
      ReadWriteIOUtils.write(lastExecutionTime, stream);
    }

    private static CQEntry deserialize(InputStream stream) throws IOException {
      String cqId = ReadWriteIOUtils.readString(stream);
      long everyInterval = ReadWriteIOUtils.readLong(stream);
      long boundaryTime = ReadWriteIOUtils.readLong(stream);
      long startTimeOffset = ReadWriteIOUtils.readLong(stream);
      long endTimeOffset = ReadWriteIOUtils.readLong(stream);
      TimeoutPolicy timeoutPolicy = TimeoutPolicy.deserialize(ReadWriteIOUtils.readByte(stream));
      String queryBody = ReadWriteIOUtils.readString(stream);
      String sql = ReadWriteIOUtils.readString(stream);
      String md5 = ReadWriteIOUtils.readString(stream);
      String zoneId = ReadWriteIOUtils.readString(stream);
      String username = ReadWriteIOUtils.readString(stream);
      CQState state = CQState.deserialize(ReadWriteIOUtils.readByte(stream));
      long lastExecutionTime = ReadWriteIOUtils.readLong(stream);
      return new CQEntry(
          cqId,
          everyInterval,
          boundaryTime,
          startTimeOffset,
          endTimeOffset,
          timeoutPolicy,
          queryBody,
          sql,
          md5,
          zoneId,
          username,
          state,
          lastExecutionTime);
    }

    public String getCqId() {
      return cqId;
    }

    public long getEveryInterval() {
      return everyInterval;
    }

    public long getBoundaryTime() {
      return boundaryTime;
    }

    public long getStartTimeOffset() {
      return startTimeOffset;
    }

    public long getEndTimeOffset() {
      return endTimeOffset;
    }

    public TimeoutPolicy getTimeoutPolicy() {
      return timeoutPolicy;
    }

    public String getQueryBody() {
      return queryBody;
    }

    public String getSql() {
      return sql;
    }

    public String getMd5() {
      return md5;
    }

    public CQState getState() {
      return state;
    }

    public long getLastExecutionTime() {
      return lastExecutionTime;
    }

    public String getZoneId() {
      return zoneId;
    }

    public String getUsername() {
      return username;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CQEntry cqEntry = (CQEntry) o;
      return everyInterval == cqEntry.everyInterval
          && boundaryTime == cqEntry.boundaryTime
          && startTimeOffset == cqEntry.startTimeOffset
          && endTimeOffset == cqEntry.endTimeOffset
          && lastExecutionTime == cqEntry.lastExecutionTime
          && Objects.equals(cqId, cqEntry.cqId)
          && timeoutPolicy == cqEntry.timeoutPolicy
          && Objects.equals(queryBody, cqEntry.queryBody)
          && Objects.equals(sql, cqEntry.sql)
          && Objects.equals(md5, cqEntry.md5)
          && Objects.equals(zoneId, cqEntry.zoneId)
          && Objects.equals(username, cqEntry.username)
          && state == cqEntry.state;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          cqId,
          everyInterval,
          boundaryTime,
          startTimeOffset,
          endTimeOffset,
          timeoutPolicy,
          queryBody,
          sql,
          md5,
          zoneId,
          username,
          state,
          lastExecutionTime);
    }
  }
}
