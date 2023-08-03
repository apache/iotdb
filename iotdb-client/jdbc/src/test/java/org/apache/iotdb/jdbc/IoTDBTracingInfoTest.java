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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class IoTDBTracingInfoTest {

  private IoTDBTracingInfo ioTDBTracingInfo;

  @Mock private TSTracingInfo tsTracingInfo;

  @Before
  public void setUp() {

    MockitoAnnotations.initMocks(this);
    ioTDBTracingInfo = new IoTDBTracingInfo();
    ioTDBTracingInfo.setTsTracingInfo(tsTracingInfo);
    assertEquals(ioTDBTracingInfo.isSetTracingInfo(), true);

    when(tsTracingInfo.getSeriesPathNum()).thenReturn(1);
    List<String> list1 = new ArrayList<>();
    list1.add("a");
    List<Long> list2 = new ArrayList<>();
    list2.add(1L);
    when(tsTracingInfo.getActivityList()).thenReturn(list1);
    when(tsTracingInfo.getElapsedTimeList()).thenReturn(list2);
    when(tsTracingInfo.getSeqFileNum()).thenReturn(1);
    when(tsTracingInfo.getUnSeqFileNum()).thenReturn(1);
    when(tsTracingInfo.getSequenceChunkNum()).thenReturn(1);
    when(tsTracingInfo.getSequenceChunkPointNum()).thenReturn(1L);
    when(tsTracingInfo.getUnsequenceChunkNum()).thenReturn(1);
    when(tsTracingInfo.getUnsequenceChunkPointNum()).thenReturn(1L);
    when(tsTracingInfo.getTotalPageNum()).thenReturn(1);

    assertEquals(ioTDBTracingInfo.getActivityList().size(), 1);
    assertEquals(ioTDBTracingInfo.getElapsedTimeList().size(), 1);
  }

  @After
  public void tearDown() {}

  @Test
  public void getStatisticsByName() throws Exception {
    assertEquals(ioTDBTracingInfo.getStatisticsByName("seriesPathNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("seqFileNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("unSeqFileNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("seqChunkNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("seqChunkPointNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("unSeqChunkNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("unSeqChunkPointNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("totalPageNum"), 1);
    assertEquals(ioTDBTracingInfo.getStatisticsByName("overlappedPageNum"), 0);
  }

  @Test
  public void getStatisticsInfoByName() throws Exception {
    assertEquals(
        ioTDBTracingInfo.getStatisticsInfoByName("seriesPathNum"), "* Num of series paths: 1");
    assertEquals(
        ioTDBTracingInfo.getStatisticsInfoByName("seqFileNum"), "* Num of sequence files read: 1");
    assertEquals(
        ioTDBTracingInfo.getStatisticsInfoByName("unSeqFileNum"),
        "* Num of unsequence files read: 1");
    assertEquals(
        ioTDBTracingInfo.getStatisticsInfoByName("seqChunkInfo"),
        "* Num of sequence chunks: 1, avg points: 1.0");
    assertEquals(
        ioTDBTracingInfo.getStatisticsInfoByName("unSeqChunkInfo"),
        "* Num of unsequence chunks: 1, avg points: 1.0");
    assertEquals(
        ioTDBTracingInfo.getStatisticsInfoByName("pageNumInfo"),
        "* Num of Pages: 1, overlapped pages: 0 (0.0%)");
  }
}
