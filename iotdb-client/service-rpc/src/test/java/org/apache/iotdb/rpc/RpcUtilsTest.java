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

package org.apache.iotdb.rpc;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class RpcUtilsTest {

  @Test
  public void parseLongToDateWithPrecision() {
    DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    ZoneId zoneId = ZoneId.of("+0000");
    Assert.assertEquals(
        "1969-12-31T23:59:59.999Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1, zoneId, "ms"));
    Assert.assertEquals(
        "1969-12-31T23:59:59.999999Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1, zoneId, "us"));
    Assert.assertEquals(
        "1969-12-31T23:59:59.999999999Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1, zoneId, "ns"));
    Assert.assertEquals(
        "1969-12-31T23:59:59.000Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1000, zoneId, "ms"));
    Assert.assertEquals(
        "1969-12-31T23:59:59.000000Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1000_000, zoneId, "us"));
    Assert.assertEquals(
        "1969-12-31T23:59:59.000000000Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1000_000_000L, zoneId, "ns"));
    Assert.assertEquals(
        "1970-01-01T00:00:00.001Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, 1, zoneId, "ms"));
    Assert.assertEquals(
        "1970-01-01T00:00:00.000001Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, 1, zoneId, "us"));
    Assert.assertEquals(
        "1970-01-01T00:00:00.000000001Z",
        RpcUtils.parseLongToDateWithPrecision(formatter, 1, zoneId, "ns"));

    zoneId = ZoneId.of("+0800");
    Assert.assertEquals(
        "1970-01-01T07:59:59.999+08:00",
        RpcUtils.parseLongToDateWithPrecision(formatter, -1, zoneId, "ms"));
  }

  @Test
  public void testIsSetSqlDialect() {
    Assert.assertTrue(RpcUtils.isSetSqlDialect("set sql_dialect=table"));
    Assert.assertTrue(RpcUtils.isSetSqlDialect("set sql_dialect =table"));
    Assert.assertTrue(RpcUtils.isSetSqlDialect("set sql_dialect  =table"));
    Assert.assertTrue(RpcUtils.isSetSqlDialect("set  sql_dialect =table"));
    Assert.assertFalse(RpcUtils.isSetSqlDialect("setsql_dialect =table"));
    Assert.assertFalse(RpcUtils.isSetSqlDialect("set           sql_dia"));
  }
}
