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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate;

import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

public class ConvertPredicateToFilterVisitorTest {

  @Test
  public void testGetValue() {
    Assert.assertEquals(
        Integer.valueOf(1),
        ConvertPredicateToFilterVisitor.<Integer>getValue("1", TSDataType.INT32));
    Assert.assertEquals(
        Long.valueOf(2), ConvertPredicateToFilterVisitor.<Long>getValue("2", TSDataType.INT64));
    Assert.assertEquals(
        Long.valueOf(3), ConvertPredicateToFilterVisitor.<Long>getValue("3", TSDataType.TIMESTAMP));
    Assert.assertEquals(
        Float.valueOf(4.5F),
        ConvertPredicateToFilterVisitor.<Float>getValue("4.5", TSDataType.FLOAT));
    Assert.assertEquals(
        Double.valueOf(6.75),
        ConvertPredicateToFilterVisitor.<Double>getValue("6.75", TSDataType.DOUBLE));
    Assert.assertEquals(
        Boolean.TRUE,
        ConvertPredicateToFilterVisitor.<Boolean>getValue("TrUe", TSDataType.BOOLEAN));
    Assert.assertEquals(
        Boolean.FALSE,
        ConvertPredicateToFilterVisitor.<Boolean>getValue("FaLsE", TSDataType.BOOLEAN));
    Assert.assertEquals(
        new Binary(new byte[] {(byte) 0xCA, (byte) 0xFE}),
        ConvertPredicateToFilterVisitor.<Binary>getValue("CAFE", TSDataType.BLOB));
    Assert.assertEquals(
        new Binary("text", TSFileConfig.STRING_CHARSET),
        ConvertPredicateToFilterVisitor.<Binary>getValue("text", TSDataType.TEXT));
    Assert.assertEquals(
        new Binary("string", TSFileConfig.STRING_CHARSET),
        ConvertPredicateToFilterVisitor.<Binary>getValue("string", TSDataType.STRING));
    Assert.assertEquals(
        Integer.valueOf(DateTimeUtils.parseDateExpressionToInt("2026-07-16")),
        ConvertPredicateToFilterVisitor.<Integer>getValue("2026-07-16", TSDataType.DATE));
  }

  @Test
  public void testGetValueWithInvalidValue() {
    IllegalArgumentException numberException =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ConvertPredicateToFilterVisitor.getValue("invalid", TSDataType.INT32));
    Assert.assertEquals(
        String.format(
            DataNodeQueryMessages.VALUE_CANNOT_BE_CAST_TO_DATA_TYPE_FMT,
            "invalid",
            TSDataType.INT32),
        numberException.getMessage());

    IllegalArgumentException booleanException =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ConvertPredicateToFilterVisitor.getValue("invalid", TSDataType.BOOLEAN));
    Assert.assertEquals(
        String.format(
            DataNodeQueryMessages.VALUE_CANNOT_BE_CAST_TO_DATA_TYPE_FMT,
            "invalid",
            TSDataType.BOOLEAN),
        booleanException.getMessage());
  }

  @Test
  public void testGetValueWithUnsupportedType() {
    UnsupportedOperationException exception =
        Assert.assertThrows(
            UnsupportedOperationException.class,
            () -> ConvertPredicateToFilterVisitor.getValue("value", TSDataType.VECTOR));
    Assert.assertEquals(
        String.format(DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, TSDataType.VECTOR),
        exception.getMessage());
  }
}
