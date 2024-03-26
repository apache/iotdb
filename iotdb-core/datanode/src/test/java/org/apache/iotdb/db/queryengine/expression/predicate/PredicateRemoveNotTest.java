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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.predicateRemoveNot;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.reversePredicate;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.between;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.eq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gte;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.in;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.longValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lte;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.neq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.not;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.notBetween;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.notIn;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.or;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.time;

public class PredicateRemoveNotTest {

  @Test
  public void testReverse() {
    // test comparison
    Assert.assertEquals(gt(time(), longValue(1)), reversePredicate(lte(time(), longValue(1))));
    Assert.assertEquals(gte(time(), longValue(1)), reversePredicate(lt(time(), longValue(1))));
    Assert.assertEquals(lt(time(), longValue(1)), reversePredicate(gte(time(), longValue(1))));
    Assert.assertEquals(lte(time(), longValue(1)), reversePredicate(gt(time(), longValue(1))));
    Assert.assertEquals(eq(time(), longValue(1)), reversePredicate(neq(time(), longValue(1))));
    Assert.assertEquals(neq(time(), longValue(1)), reversePredicate(eq(time(), longValue(1))));

    // test range
    Assert.assertEquals(
        between(time(), longValue(1), longValue(100)),
        reversePredicate(notBetween(time(), longValue(1), longValue(100))));
    Assert.assertEquals(
        notBetween(time(), longValue(1), longValue(100)),
        reversePredicate(between(time(), longValue(1), longValue(100))));

    // test set
    LinkedHashSet<String> values = new LinkedHashSet<>(Arrays.asList("a", "b"));
    Assert.assertEquals(in(time(), values), reversePredicate(notIn(time(), values)));
    Assert.assertEquals(notIn(time(), values), reversePredicate(in(time(), values)));

    // test logical
    Assert.assertEquals(gt(time(), longValue(1)), reversePredicate(not(gt(time(), longValue(1)))));
    Assert.assertEquals(
        and(gt(time(), longValue(1)), lte(time(), longValue(1))),
        reversePredicate(or(lte(time(), longValue(1)), gt(time(), longValue(1)))));
    Assert.assertEquals(
        or(gt(time(), longValue(1)), lte(time(), longValue(1))),
        reversePredicate(and(lte(time(), longValue(1)), gt(time(), longValue(1)))));
  }

  @Test
  public void testRemoveNot() {
    Assert.assertEquals(
        lte(time(), longValue(1)), predicateRemoveNot(not(gt(time(), longValue(1)))));
    Assert.assertEquals(
        between(time(), longValue(1), longValue(100)),
        predicateRemoveNot(not(notBetween(time(), longValue(1), longValue(100)))));
    Assert.assertEquals(
        or(gt(time(), longValue(1)), lte(time(), longValue(1))),
        predicateRemoveNot(or(not(lte(time(), longValue(1))), not(gt(time(), longValue(1))))));
    Assert.assertEquals(
        and(gt(time(), longValue(1)), lte(time(), longValue(1))),
        predicateRemoveNot(and(not(lte(time(), longValue(1))), not(gt(time(), longValue(1))))));
  }
}
