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

package org.apache.iotdb.db.query.control.tracing;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.service.rpc.thrift.TSTracingInfo;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A distinct TracingInfo is storaged for each query id, which includes the total number of chunks,
 * the average points number of chunk, the information of sequence files and unSequence files this
 * query involves.
 */
public class TracingInfo {

  private long startTime;

  private int seriesPathNum = 0;

  private final Set<TsFileResource> seqFileSet = new HashSet<>();
  private final Set<TsFileResource> unSeqFileSet = new HashSet<>();

  private int sequenceChunkNum = 0;
  private long sequenceChunkPoints = 0;
  private int unsequenceChunkNum = 0;
  private long unsequenceChunkPoints = 0;

  private int totalPageNum = 0;
  private int overlappedPageNum = 0;

  private final List<Pair<String, Long>> activityList = new ArrayList<>();

  public TracingInfo() {}

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setSeriesPathNum(int seriesPathNum) {
    this.seriesPathNum = seriesPathNum;
  }

  public void addChunkInfo(int totalChunkNum, long totalChunkPoints, boolean isSeq) {
    if (isSeq) {
      sequenceChunkNum += totalChunkNum;
      sequenceChunkPoints += totalChunkPoints;
    } else {
      unsequenceChunkNum += totalChunkNum;
      unsequenceChunkPoints += totalChunkPoints;
    }
  }

  public int getSequenceChunkNum() {
    return sequenceChunkNum;
  }

  public long getSequenceChunkPoints() {
    return sequenceChunkPoints;
  }

  public int getUnsequenceChunkNum() {
    return unsequenceChunkNum;
  }

  public long getUnsequenceChunkPoints() {
    return unsequenceChunkPoints;
  }

  public Set<TsFileResource> getSeqFileSet() {
    return seqFileSet;
  }

  public Set<TsFileResource> getUnSeqFileSet() {
    return unSeqFileSet;
  }

  public void addTsFileSet(List<TsFileResource> seqResources, List<TsFileResource> unSeqResources) {
    this.seqFileSet.addAll(seqResources);
    this.unSeqFileSet.addAll(unSeqResources);
  }

  public int getTotalPageNum() {
    return totalPageNum;
  }

  public int getOverlappedPageNum() {
    return overlappedPageNum;
  }

  public void addTotalPageNum(int totalPageNum) {
    this.totalPageNum += totalPageNum;
  }

  public void addOverlappedPageNum() {
    this.overlappedPageNum++;
  }

  public void registerActivity(String activity, long time) {
    activityList.add(new Pair<>(activity, time - startTime));
  }

  public void setStatisticsInfo() {
    activityList.add(
        new Pair<>(
            String.format(TracingConstant.STATISTICS_PATHNUM, seriesPathNum),
            TracingConstant.TIME_NULL));
    activityList.add(
        new Pair<>(
            String.format(TracingConstant.STATISTICS_SEQFILENUM, seqFileSet.size()),
            TracingConstant.TIME_NULL));
    activityList.add(
        new Pair<>(
            String.format(TracingConstant.STATISTICS_UNSEQFILENUM, unSeqFileSet.size()),
            TracingConstant.TIME_NULL));
    activityList.add(
        new Pair<>(
            String.format(
                TracingConstant.STATISTICS_SEQCHUNKINFO,
                sequenceChunkNum,
                (double) sequenceChunkPoints / sequenceChunkNum),
            TracingConstant.TIME_NULL));
    activityList.add(
        new Pair<>(
            String.format(
                TracingConstant.STATISTICS_UNSEQCHUNKINFO,
                unsequenceChunkNum,
                (double) unsequenceChunkPoints / unsequenceChunkNum),
            TracingConstant.TIME_NULL));
    activityList.add(
        new Pair<>(
            String.format(
                TracingConstant.STATISTICS_PAGEINFO,
                totalPageNum,
                overlappedPageNum,
                (double) overlappedPageNum / totalPageNum * 100),
            TracingConstant.TIME_NULL));
  }

  public TSTracingInfo fillRpcReturnTracingInfo() {
    TSTracingInfo tsTracingInfo = new TSTracingInfo();

    List<String> activityList = new ArrayList<>();
    List<Long> elapsedTimeList = new ArrayList<>();

    for (Pair<String, Long> pair : this.activityList) {
      String activity = pair.left;
      long elapsedTime = pair.right;

      activityList.add(activity);
      elapsedTimeList.add(elapsedTime);
    }

    tsTracingInfo.setActivityList(activityList);
    tsTracingInfo.setElapsedTimeList(elapsedTimeList);
    return tsTracingInfo;
  }
}
