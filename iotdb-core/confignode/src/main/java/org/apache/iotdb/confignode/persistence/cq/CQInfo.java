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
import org.apache.iotdb.confignode.consensus.request.read.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.consensus.response.cq.ShowCQResp;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.cq.CQCalendarUtils;
import org.apache.iotdb.confignode.rpc.thrift.TCQDuration;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  // Optional tail marker. The legacy CQ records stay byte-for-byte compatible with master.
  private static final int SNAPSHOT_EXTENSION_MARKER = 0x43515631;

  private static final String CQ_NOT_EXIST_FORMAT = "CQ %s doesn't exist.";

  private static final String CQ_TOKEN_NOT_MATCH_FORMAT = "Token of CQ %s doesn't match";

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
        res.code = TSStatusCode.CQ_ALREADY_EXIST.getStatusCode();
        res.message = String.format("CQ %s has already been created.", cqId);
      } else {
        long lastExecutionTime = plan.getFirstExecutionTime() - plan.getReq().everyInterval;
        if (plan.getReq().isSetEveryDuration()
            && plan.getReq().getEveryDuration().getMonthPart() != 0) {
          org.apache.tsfile.utils.TimeDuration duration =
              new org.apache.tsfile.utils.TimeDuration(
                  Math.toIntExact(plan.getReq().getEveryDuration().getMonthPart()),
                  plan.getReq().getEveryDuration().getNonMonthDuration());
          java.time.ZoneId zone = java.time.ZoneId.of(plan.getReq().zoneId);
          long boundary =
              plan.getReq().isSetBoundaryExplicit() && !plan.getReq().isBoundaryExplicit()
                  ? CQCalendarUtils.localEpochBoundary(zone)
                  : plan.getReq().boundaryTime;
          long index =
              CQCalendarUtils.firstOccurrenceIndex(
                  boundary, duration, plan.getFirstExecutionTime(), zone);
          lastExecutionTime = CQCalendarUtils.occurrence(boundary, duration, index - 1, zone);
        }
        long nextOccurrenceIndex = -1;
        if (plan.getReq().isSetDurationEncodingVersion()
            && plan.getReq().getDurationEncodingVersion() == 1) {
          TimeDuration duration =
              new TimeDuration(
                  Math.toIntExact(plan.getReq().getEveryDuration().getMonthPart()),
                  plan.getReq().getEveryDuration().getNonMonthDuration());
          long boundary =
              plan.getReq().isSetBoundaryExplicit() && !plan.getReq().isBoundaryExplicit()
                  ? CQCalendarUtils.localEpochBoundary(java.time.ZoneId.of(plan.getReq().zoneId))
                  : plan.getReq().boundaryTime;
          nextOccurrenceIndex =
              CQCalendarUtils.firstOccurrenceIndex(
                  boundary,
                  duration,
                  plan.getFirstExecutionTime(),
                  java.time.ZoneId.of(plan.getReq().zoneId));
        }
        CQEntry cqEntry =
            new CQEntry(plan.getReq(), plan.getCqToken(), lastExecutionTime, nextOccurrenceIndex);
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
   * @return SUCCESS_STATUS if there is CQ whose ID and token is same as <tt>cqId</tt> in plan,
   *     otherwise NO_SUCH_CQ.
   */
  public TSStatus dropCQ(DropCQPlan plan) {
    TSStatus res = new TSStatus();
    String cqId = plan.getCqId();
    Optional<String> cqToken = plan.getCqToken();
    lock.writeLock().lock();
    try {
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format(CQ_NOT_EXIST_FORMAT, cqId);
        LOGGER.warn(ConfigNodeMessages.DROP_CQ_FAILED_BECAUSE_IT_DOESN_T_EXIST, cqId);
      } else if ((cqToken.isPresent() && !cqToken.get().equals(cqEntry.cqToken))) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format(CQ_TOKEN_NOT_MATCH_FORMAT, cqId);
        LOGGER.warn(ConfigNodeMessages.DROP_CQ_FAILED_BECAUSE_ITS_TOKEN_DOESN_T_MATCH, cqId);
      } else {
        cqMap.remove(cqId);
        res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
        LOGGER.info(ConfigNodeMessages.DROP_CQ_SUCCESSFULLY, cqId);
      }
      return res;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ShowCQResp showCQ() {
    return showCQ(new ShowCQPlan());
  }

  public ShowCQResp showCQ(ShowCQPlan plan) {
    lock.readLock().lock();
    try {
      Optional<String> cqId = plan.getCqId();
      List<CQEntry> cqList;
      if (cqId.isPresent()) {
        CQEntry cqEntry = cqMap.get(cqId.get());
        cqList =
            cqEntry == null
                ? Collections.emptyList()
                : Collections.singletonList(new CQEntry(cqEntry));
      } else {
        cqList = cqMap.values().stream().map(CQEntry::new).collect(Collectors.toList());
      }
      return new ShowCQResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), cqList);
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
    String cqToken = plan.getCqToken();
    lock.writeLock().lock();
    try {
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format(CQ_NOT_EXIST_FORMAT, cqId);
      } else if (!cqToken.equals(cqEntry.cqToken)) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format(CQ_TOKEN_NOT_MATCH_FORMAT, cqId);
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
   *     token is different. or CQ_UPDATE_LAST_EXEC_TIME_FAILED 3. original lastExecutionTime >=
   *     current lastExecutionTime;
   */
  public TSStatus updateCQLastExecutionTime(UpdateCQLastExecTimePlan plan) {
    TSStatus res = new TSStatus();
    String cqId = plan.getCqId();
    String cqToken = plan.getCqToken();
    lock.writeLock().lock();
    try {
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format(CQ_NOT_EXIST_FORMAT, cqId);
      } else if (!cqToken.equals(cqEntry.cqToken)) {
        res.code = TSStatusCode.NO_SUCH_CQ.getStatusCode();
        res.message = String.format(CQ_TOKEN_NOT_MATCH_FORMAT, cqId);
      } else if (plan.hasOccurrenceIndex()) {
        if (cqEntry.nextOccurrenceIndex < 0) {
          res.code = TSStatusCode.CQ_UPDATE_LAST_EXEC_TIME_ERROR.getStatusCode();
          res.message = ManagerMessages.MESSAGE_CQ_DOES_NOT_HAVE_OCCURRENCE_INDEX_METADATA_929A7F0C;
        } else if (cqEntry.nextOccurrenceIndex > plan.getExpectedIndex()) {
          res.code = TSStatusCode.CQ_UPDATE_LAST_EXEC_TIME_ERROR.getStatusCode();
          res.message = ManagerMessages.MESSAGE_CQ_OCCURRENCE_CALLBACK_IS_STALE_36C5FBFC;
        } else if (cqEntry.nextOccurrenceIndex < plan.getExpectedIndex()) {
          res.code = TSStatusCode.CQ_UPDATE_LAST_EXEC_TIME_ERROR.getStatusCode();
          res.message =
              ManagerMessages.MESSAGE_CQ_OCCURRENCE_INDEX_IS_AHEAD_OF_THE_CALLBACK_8A18ECC9;
        } else {
          cqEntry.nextOccurrenceIndex = plan.getTargetIndex();
          cqEntry.lastExecutionTime = plan.getExecutionTime();
          res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
        }
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
          ConfigNodeMessages.FAILED_TO_TAKE_SNAPSHOT_OF_CQINFO_BECAUSE_SNAPSHOT_FILE_IS,
          snapshotFile.getAbsolutePath());
      return false;
    }

    lock.readLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serialize(fileOutputStream);
      fileOutputStream.getFD().sync();
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(cqMap.size(), stream);
    for (CQEntry entry : cqMap.values()) {
      entry.serializeLegacy(stream);
    }
    ReadWriteIOUtils.write(SNAPSHOT_EXTENSION_MARKER, stream);
    ReadWriteIOUtils.write(cqMap.size(), stream);
    for (CQEntry entry : cqMap.values()) {
      entry.serializeExtension(stream);
    }
  }

  private void deserialize(InputStream stream) throws IOException {
    int size = ReadWriteIOUtils.readInt(stream);
    if (size < 0) {
      throw new IOException(
          String.format(
              ManagerMessages.EXCEPTION_NEGATIVE_CQ_SNAPSHOT_ENTRY_COUNT_ARG_38750035, size));
    }
    for (int i = 0; i < size; i++) {
      CQEntry cqEntry = CQEntry.deserializeLegacy(stream);
      cqMap.put(cqEntry.cqId, cqEntry);
    }
    if (stream.available() < Integer.BYTES
        || ReadWriteIOUtils.readInt(stream) != SNAPSHOT_EXTENSION_MARKER) {
      return;
    }
    int extensionSize = ReadWriteIOUtils.readInt(stream);
    if (extensionSize < 0) {
      throw new IOException(
          String.format(
              ManagerMessages.EXCEPTION_NEGATIVE_CQ_SNAPSHOT_ENTRY_COUNT_ARG_38750035,
              extensionSize));
    }
    for (int i = 0; i < extensionSize; i++) {
      String cqId = ReadWriteIOUtils.readString(stream);
      CQEntry cqEntry = cqMap.get(cqId);
      if (cqEntry == null) {
        CQEntry.skipExtension(stream);
      } else {
        cqEntry.deserializeExtension(stream);
      }
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          ConfigNodeMessages.FAILED_TO_LOAD_SNAPSHOT_OF_CQINFO_SNAPSHOT_FILE_DOES_NOT,
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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
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
    private final String cqToken;

    private final String zoneId;

    private final String username;
    private org.apache.tsfile.utils.TimeDuration everyDuration;
    private org.apache.tsfile.utils.TimeDuration startTimeOffsetDuration;
    private org.apache.tsfile.utils.TimeDuration endTimeOffsetDuration;
    private boolean boundaryExplicit;

    private CQState state;
    private long lastExecutionTime;
    private long nextOccurrenceIndex;

    private CQEntry(TCreateCQReq req, String cqToken, long lastExecutionTime) {
      this(req, cqToken, lastExecutionTime, -1);
    }

    private CQEntry(
        TCreateCQReq req, String cqToken, long lastExecutionTime, long nextOccurrenceIndex) {
      this(
          req.cqId,
          req.everyInterval,
          req.boundaryTime,
          req.startTimeOffset,
          req.endTimeOffset,
          TimeoutPolicy.deserialize(req.timeoutPolicy),
          req.queryBody,
          req.sql,
          cqToken,
          req.zoneId,
          req.username,
          durationFromReq(
              req, req.isSetEveryDuration() ? req.getEveryDuration() : null, req.everyInterval),
          durationFromReq(
              req,
              req.isSetStartOffsetDuration() ? req.getStartOffsetDuration() : null,
              req.startTimeOffset),
          durationFromReq(
              req,
              req.isSetEndOffsetDuration() ? req.getEndOffsetDuration() : null,
              req.endTimeOffset),
          req.isSetBoundaryExplicit() && req.isBoundaryExplicit(),
          CQState.INACTIVE,
          lastExecutionTime,
          nextOccurrenceIndex);
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
          other.cqToken,
          other.zoneId,
          other.username,
          other.everyDuration,
          other.startTimeOffsetDuration,
          other.endTimeOffsetDuration,
          other.boundaryExplicit,
          other.state,
          other.lastExecutionTime,
          other.nextOccurrenceIndex);
    }

    @SuppressWarnings("squid:S107")
    private CQEntry(
        String cqId,
        long everyInterval,
        long boundaryTime,
        long startTimeOffset,
        long endTimeOffset,
        TimeoutPolicy timeoutPolicy,
        String queryBody,
        String sql,
        String cqToken,
        String zoneId,
        String username,
        org.apache.tsfile.utils.TimeDuration everyDuration,
        org.apache.tsfile.utils.TimeDuration startTimeOffsetDuration,
        org.apache.tsfile.utils.TimeDuration endTimeOffsetDuration,
        boolean boundaryExplicit,
        CQState state,
        long lastExecutionTime,
        long nextOccurrenceIndex) {
      this.cqId = cqId;
      this.everyInterval = everyInterval;
      this.boundaryTime = boundaryTime;
      this.startTimeOffset = startTimeOffset;
      this.endTimeOffset = endTimeOffset;
      this.timeoutPolicy = timeoutPolicy;
      this.queryBody = queryBody;
      this.sql = sql;
      this.cqToken = cqToken;
      this.zoneId = zoneId;
      this.username = username;
      this.everyDuration = everyDuration;
      this.startTimeOffsetDuration = startTimeOffsetDuration;
      this.endTimeOffsetDuration = endTimeOffsetDuration;
      this.boundaryExplicit = boundaryExplicit;
      this.state = state;
      this.lastExecutionTime = lastExecutionTime;
      this.nextOccurrenceIndex = nextOccurrenceIndex;
    }

    private void serializeLegacy(OutputStream stream) throws IOException {
      ReadWriteIOUtils.write(cqId, stream);
      ReadWriteIOUtils.write(everyInterval, stream);
      ReadWriteIOUtils.write(boundaryTime, stream);
      ReadWriteIOUtils.write(startTimeOffset, stream);
      ReadWriteIOUtils.write(endTimeOffset, stream);
      ReadWriteIOUtils.write(timeoutPolicy.getType(), stream);
      ReadWriteIOUtils.write(queryBody, stream);
      ReadWriteIOUtils.write(sql, stream);
      ReadWriteIOUtils.write(cqToken, stream);
      ReadWriteIOUtils.write(zoneId, stream);
      ReadWriteIOUtils.write(username, stream);
      ReadWriteIOUtils.write(state.getType(), stream);
      ReadWriteIOUtils.write(lastExecutionTime, stream);
    }

    private void serializeExtension(OutputStream stream) throws IOException {
      ReadWriteIOUtils.write(cqId, stream);
      ReadWriteIOUtils.write(everyDuration.monthDuration, stream);
      ReadWriteIOUtils.write(everyDuration.nonMonthDuration, stream);
      ReadWriteIOUtils.write(startTimeOffsetDuration.monthDuration, stream);
      ReadWriteIOUtils.write(startTimeOffsetDuration.nonMonthDuration, stream);
      ReadWriteIOUtils.write(endTimeOffsetDuration.monthDuration, stream);
      ReadWriteIOUtils.write(endTimeOffsetDuration.nonMonthDuration, stream);
      ReadWriteIOUtils.write(boundaryExplicit, stream);
      ReadWriteIOUtils.write(nextOccurrenceIndex, stream);
    }

    private static CQEntry deserializeLegacy(InputStream stream) throws IOException {
      String cqId = ReadWriteIOUtils.readString(stream);
      long everyInterval = ReadWriteIOUtils.readLong(stream);
      long boundaryTime = ReadWriteIOUtils.readLong(stream);
      long startTimeOffset = ReadWriteIOUtils.readLong(stream);
      long endTimeOffset = ReadWriteIOUtils.readLong(stream);
      TimeoutPolicy timeoutPolicy = TimeoutPolicy.deserialize(ReadWriteIOUtils.readByte(stream));
      String queryBody = ReadWriteIOUtils.readString(stream);
      String sql = ReadWriteIOUtils.readString(stream);
      String cqToken = ReadWriteIOUtils.readString(stream);
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
          cqToken,
          zoneId,
          username,
          new org.apache.tsfile.utils.TimeDuration(0, everyInterval),
          new org.apache.tsfile.utils.TimeDuration(0, startTimeOffset),
          new org.apache.tsfile.utils.TimeDuration(0, endTimeOffset),
          false,
          state,
          lastExecutionTime,
          -1);
    }

    private void deserializeExtension(InputStream stream) throws IOException {
      everyDuration =
          new org.apache.tsfile.utils.TimeDuration(
              ReadWriteIOUtils.readInt(stream), ReadWriteIOUtils.readLong(stream));
      startTimeOffsetDuration =
          new org.apache.tsfile.utils.TimeDuration(
              ReadWriteIOUtils.readInt(stream), ReadWriteIOUtils.readLong(stream));
      endTimeOffsetDuration =
          new org.apache.tsfile.utils.TimeDuration(
              ReadWriteIOUtils.readInt(stream), ReadWriteIOUtils.readLong(stream));
      boundaryExplicit = ReadWriteIOUtils.readBool(stream);
      nextOccurrenceIndex = ReadWriteIOUtils.readLong(stream);
    }

    private static void skipExtension(InputStream stream) throws IOException {
      ReadWriteIOUtils.readInt(stream);
      ReadWriteIOUtils.readLong(stream);
      ReadWriteIOUtils.readInt(stream);
      ReadWriteIOUtils.readLong(stream);
      ReadWriteIOUtils.readInt(stream);
      ReadWriteIOUtils.readLong(stream);
      ReadWriteIOUtils.readBool(stream);
      ReadWriteIOUtils.readLong(stream);
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

    public String getCqToken() {
      return cqToken;
    }

    public CQState getState() {
      return state;
    }

    public long getLastExecutionTime() {
      return lastExecutionTime;
    }

    public long getNextOccurrenceIndex() {
      return nextOccurrenceIndex;
    }

    public String getZoneId() {
      return zoneId;
    }

    public String getUsername() {
      return username;
    }

    public org.apache.tsfile.utils.TimeDuration getEveryDuration() {
      return everyDuration;
    }

    public org.apache.tsfile.utils.TimeDuration getStartTimeOffsetDuration() {
      return startTimeOffsetDuration;
    }

    public org.apache.tsfile.utils.TimeDuration getEndTimeOffsetDuration() {
      return endTimeOffsetDuration;
    }

    public boolean isBoundaryExplicit() {
      return boundaryExplicit;
    }

    private static org.apache.tsfile.utils.TimeDuration durationFromReq(
        TCreateCQReq req, TCQDuration d, long legacy) {
      if (req.isSetDurationEncodingVersion()
          && req.getDurationEncodingVersion() == 1
          && d != null) {
        return new org.apache.tsfile.utils.TimeDuration(
            Math.toIntExact(d.getMonthPart()), d.getNonMonthDuration());
      }
      return new org.apache.tsfile.utils.TimeDuration(0, legacy);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CQEntry cqEntry = (CQEntry) o;
      return everyInterval == cqEntry.everyInterval
          && boundaryTime == cqEntry.boundaryTime
          && startTimeOffset == cqEntry.startTimeOffset
          && endTimeOffset == cqEntry.endTimeOffset
          && lastExecutionTime == cqEntry.lastExecutionTime
          && nextOccurrenceIndex == cqEntry.nextOccurrenceIndex
          && Objects.equals(cqId, cqEntry.cqId)
          && timeoutPolicy == cqEntry.timeoutPolicy
          && Objects.equals(queryBody, cqEntry.queryBody)
          && Objects.equals(sql, cqEntry.sql)
          && Objects.equals(cqToken, cqEntry.cqToken)
          && Objects.equals(zoneId, cqEntry.zoneId)
          && Objects.equals(username, cqEntry.username)
          && Objects.equals(everyDuration, cqEntry.everyDuration)
          && Objects.equals(startTimeOffsetDuration, cqEntry.startTimeOffsetDuration)
          && Objects.equals(endTimeOffsetDuration, cqEntry.endTimeOffsetDuration)
          && boundaryExplicit == cqEntry.boundaryExplicit
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
          cqToken,
          zoneId,
          username,
          everyDuration,
          startTimeOffsetDuration,
          endTimeOffsetDuration,
          boundaryExplicit,
          state,
          lastExecutionTime,
          nextOccurrenceIndex);
    }
  }
}
