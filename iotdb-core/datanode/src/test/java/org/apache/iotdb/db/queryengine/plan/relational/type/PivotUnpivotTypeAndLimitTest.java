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

package org.apache.iotdb.db.queryengine.plan.relational.type;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PivotLimitValidator;

import org.apache.tsfile.read.common.type.Type;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class PivotUnpivotTypeAndLimitTest {
  @Test
  public void testOutputWidthAndEmptyDynamicValues() {
    assertEquals(BigInteger.valueOf(13), PivotLimitValidator.validate(1, 3, List.of(2, 2), 13));
    assertThrows(
        SemanticException.class, () -> PivotLimitValidator.validate(1, 3, List.of(2, 2), 12));
    assertEquals(BigInteger.ONE, PivotLimitValidator.validate(1, 3, List.of(1000, 0), 1));
    List<Integer> huge = List.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertThrows(
        SemanticException.class, () -> PivotLimitValidator.validate(1, 2, huge, Integer.MAX_VALUE));
  }

  @Test
  public void testOnlyWhitelistedTypeCombinations() {
    assertEquals(INT64, common(INT32, INT64));
    assertEquals(FLOAT, common(INT32, FLOAT));
    assertEquals(DOUBLE, common(INT64, DOUBLE));
    assertEquals(DOUBLE, common(FLOAT, DOUBLE));
    assertEquals(STRING, common(TEXT, STRING));
    assertEquals(TIMESTAMP, common(TIMESTAMP, TIMESTAMP));
    assertFalse(UnpivotTypeUnifier.getCommonType(INT64, TIMESTAMP).isPresent());
    assertFalse(UnpivotTypeUnifier.getCommonType(INT32, STRING).isPresent());
  }

  private Type common(Type left, Type right) {
    assertEquals(
        UnpivotTypeUnifier.getCommonType(left, right),
        UnpivotTypeUnifier.getCommonType(right, left));
    return UnpivotTypeUnifier.getCommonType(left, right).orElseThrow();
  }
}
