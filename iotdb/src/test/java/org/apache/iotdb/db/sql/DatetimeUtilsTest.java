/**
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
package org.apache.iotdb.db.sql;

import static org.junit.Assert.assertEquals;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatetimeUtilsTest {

  private ZoneOffset zoneOffset;
  private ZoneId zoneId;
  // 1546413207689
  // 2019-01-02T15:13:27.689+08:00
  private final long timestamp = 1546413207689L;
  private long delta;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test1() throws LogicalOperatorException{
    zoneOffset = ZonedDateTime.now().getOffset();
    zoneId = ZoneId.systemDefault();
    if(zoneOffset.toString().equals("Z")){
      delta = 8 * 3600000;
    } else {
      delta = (8 - Long.parseLong(zoneOffset.toString().split(":")[0])) * 3600000;
    }
    testConvertDatetimeStrToLongWithoutMS(zoneOffset, zoneId, timestamp - 689 + delta);
    testConvertDatetimeStrToLongWithMS(zoneOffset, zoneId, timestamp + delta);
  }

  @Test
  public void test2() throws LogicalOperatorException{
    zoneOffset = ZoneOffset.UTC;
    zoneId = ZoneId.of("Etc/UTC");
    delta = 8 * 3600000;
    testConvertDatetimeStrToLongWithoutMS(zoneOffset, zoneId, timestamp - 689 + delta);
    testConvertDatetimeStrToLongWithMS(zoneOffset, zoneId, timestamp + delta);
  }

  public void testConvertDatetimeStrToLongWithoutMS(ZoneOffset zoneOffset, ZoneId zoneId, long res) throws LogicalOperatorException {
    String[] timeFormatWithoutMs = new String[]{"2019-01-02 15:13:27", "2019/01/02 15:13:27",
        "2019.01.02 15:13:27", "2019-01-02T15:13:27", "2019/01/02T15:13:27", "2019.01.02T15:13:27",
        "2019-01-02 15:13:27" + zoneOffset, "2019/01/02 15:13:27" + zoneOffset,
        "2019.01.02 15:13:27" + zoneOffset, "2019-01-02T15:13:27" + zoneOffset,
        "2019/01/02T15:13:27" + zoneOffset, "2019.01.02T15:13:27" + zoneOffset,};
    for (String str : timeFormatWithoutMs) {
      Assert.assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneOffset, 0));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneId));
    }

  }

  public void testConvertDatetimeStrToLongWithMS(ZoneOffset zoneOffset, ZoneId zoneId, long res) throws LogicalOperatorException {
    String[] timeFormatWithoutMs = new String[]{"2019-01-02 15:13:27.689",
        "2019/01/02 15:13:27.689",
        "2019.01.02 15:13:27.689", "2019-01-02T15:13:27.689", "2019/01/02T15:13:27.689",
        "2019.01.02T15:13:27.689", "2019-01-02 15:13:27.689" + zoneOffset,
        "2019/01/02 15:13:27.689" + zoneOffset, "2019.01.02 15:13:27.689" + zoneOffset,
        "2019-01-02T15:13:27.689" + zoneOffset, "2019/01/02T15:13:27.689" + zoneOffset,
        "2019.01.02T15:13:27.689" + zoneOffset,};
    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneOffset, 0));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneId));
    }
  }

  public void createTest() {
    // long timestamp = System.currentTimeMillis();
    // System.out.println(timestamp);
    // ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("+08:00"));
    // System.out.println(zonedDateTime);
  }

  public static void main(String[] args){
//    System.out.println(DatetimeUtils.toZoneOffset(ZoneId.of("Etc/UTC")));
    for(String zoneId : ZoneId.getAvailableZoneIds()){
      System.out.println(zoneId + ": " + DatetimeUtils.toZoneOffset(ZoneId.of(zoneId)));
    }
//	  System.out.println(ZoneOffset.of("+00:00"));
  }
}
