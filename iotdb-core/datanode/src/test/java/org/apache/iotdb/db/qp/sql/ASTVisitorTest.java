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

package org.apache.iotdb.db.qp.sql;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class ASTVisitorTest {

  ASTVisitor visitor;

  @Before
  public void setUp() {
    visitor = new ASTVisitor();
    visitor.setZoneId(ZonedDateTime.now().getOffset());
  }

  @After
  public void tearDown() {}

  @Test
  public void testParseTimeFormatNow() {
    long now = visitor.parseDateFormat(SqlConstant.NOW_FUNC);
    for (int i = 0; i <= 12; i++) {
      ZoneOffset offset1, offset2;
      if (i < 10) {
        offset1 = ZoneOffset.of("+0" + i + ":00");
        offset2 = ZoneOffset.of("-0" + i + ":00");
      } else {
        offset1 = ZoneOffset.of("+" + i + ":00");
        offset2 = ZoneOffset.of("-" + i + ":00");
      }
      ZonedDateTime zonedDateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of(offset1.toString()));
      assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
      zonedDateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of(offset2.toString()));
      assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
    }
  }

  @Test
  public void testParseTimeFormatNowPrecision() {
    String timePrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    CommonDescriptor.getInstance().getConfig().setTimestampPrecision("ms");
    long now_ms = visitor.parseDateFormat(SqlConstant.NOW_FUNC);
    String ms_str = String.valueOf(now_ms);

    CommonDescriptor.getInstance().getConfig().setTimestampPrecision("us");
    long now_us = visitor.parseDateFormat(SqlConstant.NOW_FUNC);
    String us_str = String.valueOf(now_us);

    CommonDescriptor.getInstance().getConfig().setTimestampPrecision("ns");
    long now_ns = visitor.parseDateFormat(SqlConstant.NOW_FUNC);
    String ns_str = String.valueOf(now_ns);

    assertEquals(ms_str.length() + 3, (us_str).length());
    assertEquals(us_str.length() + 3, (ns_str).length());
    CommonDescriptor.getInstance().getConfig().setTimestampPrecision(timePrecision);
  }

  @Test(expected = SemanticException.class)
  public void testParseTimeFormatFail1() {
    visitor.parseDateFormat(null);
  }

  @Test(expected = SemanticException.class)
  public void testParseTimeFormatFail2() {
    visitor.parseDateFormat("");
  }
}
