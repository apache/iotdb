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

package org.apache.iotdb.db.queryengine.expression.predicate;

import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.add;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;
import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.schemaMap;

public class PredicateUtilsTest {

  @Test
  public void testExtractPredicateSourceSymbol() {
    Assert.assertEquals(
        schemaMap.get("root.sg.d1.s1"),
        PredicateUtils.extractPredicateSourceSymbol(
            gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10"))));
    Assert.assertEquals(
        schemaMap.get("root.sg.d2.a.s1").getDevicePath(),
        PredicateUtils.extractPredicateSourceSymbol(
            gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10"))));
    Assert.assertEquals(
        schemaMap.get("root.sg.d1.s1"),
        PredicateUtils.extractPredicateSourceSymbol(
            and(
                gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                gt(
                    add(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("1")),
                    intValue("10")))));
    Assert.assertNull(
        PredicateUtils.extractPredicateSourceSymbol(
            and(
                gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                gt(
                    add(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("1")),
                    intValue("10")))));
    Assert.assertNull(
        PredicateUtils.extractPredicateSourceSymbol(
            and(
                gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                gt(
                    add(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("1")),
                    intValue("10")))));
    Assert.assertEquals(
        schemaMap.get("root.sg.d2.a.s1").getDevicePath(),
        PredicateUtils.extractPredicateSourceSymbol(
            and(
                gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                gt(
                    add(timeSeries(schemaMap.get("root.sg.d2.a.s2")), intValue("1")),
                    intValue("10")))));
  }
}
