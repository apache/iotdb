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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.add;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.between;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.eq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gte;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.in;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.isNotNull;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.isNull;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.like;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.longValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.not;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.notRegex;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.sin;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.time;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;

public class PredicatePushIntoScanTest {

  private static final TimeSeriesOperand testPath = timeSeries(new PartialPath("s1", false));

  @Test
  public void testCanPushInto() {
    // test comparison
    testCanPushIntoScan(lt(testPath, intValue("1")));
    testCanPushIntoScan(lt(intValue("1"), testPath));
    testCanPushIntoScan(gte(time(), longValue(1)));
    testCanPushIntoScan(gte(longValue(1), time()));

    // test between
    testCanPushIntoScan(between(testPath, intValue("1"), intValue("2")));
    testCanPushIntoScan(between(intValue("1"), testPath, intValue("2")));
    testCanPushIntoScan(between(intValue("1"), intValue("2"), testPath));
    testCanPushIntoScan(between(time(), longValue(1), longValue(2)));
    testCanPushIntoScan(between(longValue(1), time(), longValue(2)));
    testCanPushIntoScan(between(longValue(1), longValue(2), time()));

    // test in
    LinkedHashSet<String> values = new LinkedHashSet<>(Arrays.asList("1", "2", "3"));
    testCanPushIntoScan(in(testPath, values));

    // test is not null
    testCanPushIntoScan(isNotNull(testPath));

    // test like/regExp
    testCanPushIntoScan(like(testPath, "%cc%"));
    testCanPushIntoScan(notRegex(testPath, "^[A-Za-z]+$"));
  }

  @Test
  public void testCannotPushInto() {
    // test comparison
    testCannotPushIntoScan(eq(add(testPath, intValue("1")), intValue("1")));
    testCannotPushIntoScan(eq(intValue("1"), add(testPath, intValue("1"))));
    testCannotPushIntoScan(eq(sin(testPath), intValue("1")));
    testCannotPushIntoScan(eq(intValue("1"), sin(testPath)));
    testCannotPushIntoScan(lt(testPath, add(intValue("1"), intValue("1"))));
    testCannotPushIntoScan(lt(testPath, testPath));

    // test between
    testCannotPushIntoScan(between(testPath, intValue("1"), add(intValue("1"), intValue("1"))));
    testCannotPushIntoScan(between(testPath, intValue("1"), testPath));
    testCannotPushIntoScan(between(testPath, testPath, intValue("1")));
    testCannotPushIntoScan(between(testPath, testPath, testPath));
    testCannotPushIntoScan(between(testPath, intValue("1"), sin(testPath)));
    testCannotPushIntoScan(between(testPath, sin(testPath), intValue("1")));

    // test in
    LinkedHashSet<String> values = new LinkedHashSet<>(Arrays.asList("1", "2", "3"));
    testCannotPushIntoScan(in(sin(testPath), values));
    testCannotPushIntoScan(in(add(testPath, intValue("1")), values));

    // test is not null
    testCannotPushIntoScan(isNotNull(sin(testPath)));
    testCannotPushIntoScan(isNotNull(add(testPath, intValue("1"))));

    // test like/regExp
    testCannotPushIntoScan(like(sin(testPath), "%cc%"));
    testCannotPushIntoScan(notRegex(sin(testPath), "^[A-Za-z]+$"));
    testCannotPushIntoScan(like(add(testPath, intValue("1")), "%cc%"));
    testCannotPushIntoScan(notRegex(add(testPath, intValue("1")), "^[A-Za-z]+$"));
  }

  @Test
  public void testIllegalPredicate() {
    testIllegalPredicate(isNull(testPath));

    testIllegalPredicate(not(eq(testPath, intValue("1"))));

    testIllegalPredicate(like(time(), "%cc%"));
    testIllegalPredicate(notRegex(time(), "^[A-Za-z]+$"));
    testIllegalPredicate(isNull(time()));
    testIllegalPredicate(isNotNull(time()));
  }

  private void testCanPushIntoScan(Expression predicate) {
    Assert.assertTrue(PredicateUtils.predicateCanPushIntoScan(predicate));
  }

  private void testCannotPushIntoScan(Expression predicate) {
    Assert.assertFalse(PredicateUtils.predicateCanPushIntoScan(predicate));
  }

  private void testIllegalPredicate(Expression predicate) {
    Assert.assertThrows(
        IllegalArgumentException.class, () -> PredicateUtils.predicateCanPushIntoScan(predicate));
  }
}
