/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  private long delta;

  @Before
  public void setUp() throws Exception {
    zoneOffset = ZonedDateTime.now().getOffset();
    zoneId = ZoneId.systemDefault();
    delta = (8 - Long.parseLong(zoneOffset.toString().split(":")[0])) * 3600000;
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testConvertDatetimeStrToLongWithoutMS() throws LogicalOperatorException {
    String[] timeFormatWithoutMs = new String[]{"2019-01-02 15:13:27", "2019/01/02 15:13:27",
        "2019.01.02 15:13:27", "2019-01-02T15:13:27", "2019/01/02T15:13:27", "2019.01.02T15:13:27",
        "2019-01-02 15:13:27" + zoneOffset, "2019/01/02 15:13:27" + zoneOffset,
        "2019.01.02 15:13:27" + zoneOffset, "2019-01-02T15:13:27" + zoneOffset,
        "2019/01/02T15:13:27" + zoneOffset, "2019.01.02T15:13:27" + zoneOffset,};

    long res = 1546413207000L + delta;
    for (String str : timeFormatWithoutMs) {
      Assert.assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneOffset));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneId));
    }

  }

  @Test
  public void testConvertDatetimeStrToLongWithMS() throws LogicalOperatorException {
    String[] timeFormatWithoutMs = new String[]{"2019-01-02 15:13:27.689",
        "2019/01/02 15:13:27.689",
        "2019.01.02 15:13:27.689", "2019-01-02T15:13:27.689", "2019/01/02T15:13:27.689",
        "2019.01.02T15:13:27.689", "2019-01-02 15:13:27.689" + zoneOffset,
        "2019/01/02 15:13:27.689" + zoneOffset, "2019.01.02 15:13:27.689" + zoneOffset,
        "2019-01-02T15:13:27.689" + zoneOffset, "2019/01/02T15:13:27.689" + zoneOffset,
        "2019.01.02T15:13:27.689" + zoneOffset,};

    long res = 1546413207689L + delta;
    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneOffset));
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
}
