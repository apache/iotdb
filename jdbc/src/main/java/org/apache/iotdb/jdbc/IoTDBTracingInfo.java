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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.service.rpc.thrift.TSTracingInfo;

import java.util.List;

public class IoTDBTracingInfo {

  private TSTracingInfo tsTracingInfo;

  public IoTDBTracingInfo() {}

  public void setTsTracingInfo(TSTracingInfo tsTracingInfo) {
    this.tsTracingInfo = tsTracingInfo;
  }

  public boolean isSetTracingInfo() {
    return tsTracingInfo != null;
  }

  public List<String> getActivityList() {
    return tsTracingInfo.getActivityList();
  }

  public List<Long> getElapsedTimeList() {
    return tsTracingInfo.getElapsedTimeList();
  }

  public long getStatisticsByName(String name) throws Exception {
    switch (name) {
      case "seriesPathNum":
        return tsTracingInfo.getSeriesPathNum();
      case "seqFileNum":
        return tsTracingInfo.getSeqFileNum();
      case "unSeqFileNum":
        return tsTracingInfo.getUnSeqFileNum();
      case "seqChunkNum":
        return tsTracingInfo.getSequenceChunkNum();
      case "seqChunkPointNum":
        return tsTracingInfo.getSequenceChunkPointNum();
      case "unSeqChunkNum":
        return tsTracingInfo.getUnsequenceChunkNum();
      case "unSeqChunkPointNum":
        return tsTracingInfo.getUnsequenceChunkPointNum();
      case "totalPageNum":
        return tsTracingInfo.getTotalPageNum();
      case "overlappedPageNum":
        return tsTracingInfo.getOverlappedPageNum();
      default:
        throw new Exception("Invalid statistics name!");
    }
  }

  public String getStatisticsInfoByName(String name) throws Exception {
    switch (name) {
      case "seriesPathNum":
        return String.format(Constant.STATISTICS_PATHNUM, tsTracingInfo.getSeriesPathNum());
      case "seqFileNum":
        return String.format(Constant.STATISTICS_SEQFILENUM, tsTracingInfo.getSeqFileNum());
      case "unSeqFileNum":
        return String.format(Constant.STATISTICS_UNSEQFILENUM, tsTracingInfo.getUnSeqFileNum());
      case "seqChunkInfo":
        return String.format(
            Constant.STATISTICS_SEQCHUNKINFO,
            tsTracingInfo.getSequenceChunkNum(),
            (double) tsTracingInfo.getSequenceChunkPointNum()
                / tsTracingInfo.getSequenceChunkNum());
      case "unSeqChunkInfo":
        return String.format(
            Constant.STATISTICS_UNSEQCHUNKINFO,
            tsTracingInfo.getUnsequenceChunkNum(),
            (double) tsTracingInfo.getUnsequenceChunkPointNum()
                / tsTracingInfo.getUnsequenceChunkNum());
      case "pageNumInfo":
        return String.format(
            Constant.STATISTICS_PAGEINFO,
            tsTracingInfo.getTotalPageNum(),
            tsTracingInfo.getOverlappedPageNum(),
            (double) tsTracingInfo.getOverlappedPageNum() / tsTracingInfo.getTotalPageNum() * 100);
      default:
        throw new Exception("Invalid statistics name!");
    }
  }
}
