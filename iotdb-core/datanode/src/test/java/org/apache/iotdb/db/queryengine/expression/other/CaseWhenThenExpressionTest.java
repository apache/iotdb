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

package org.apache.iotdb.db.queryengine.expression.other;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.eq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.whenThen;
import static org.junit.Assert.assertEquals;

public class CaseWhenThenExpressionTest {

  private void assertSerializeDeserializeEqual(CaseWhenThenExpression caseWhenThenExpression) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
    byteBuffer.mark();
    Expression.serialize(caseWhenThenExpression, byteBuffer);
    byteBuffer.reset();
    assertEquals(caseWhenThenExpression, Expression.deserialize(byteBuffer));
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException {
    // normal: case when x=1 then 10 else 20 end
    List<WhenThenExpression> whenThenExpressionList = new ArrayList<>();
    whenThenExpressionList.add(whenThen(eq(timeSeries("root.f.x"), intValue("1")), intValue("10")));
    CaseWhenThenExpression case1 =
        new CaseWhenThenExpression(whenThenExpressionList, intValue("20"));
    assertSerializeDeserializeEqual(case1);

    // 4 WHEN-THENs
    whenThenExpressionList.add(whenThen(gt(timeSeries("root.f.x"), intValue("1")), intValue("10")));
    whenThenExpressionList.add(whenThen(lt(timeSeries("root.f.y"), intValue("2")), intValue("20")));
    whenThenExpressionList.add(
        whenThen(eq(timeSeries("root.f.g"), intValue("333")), intValue("20")));
    CaseWhenThenExpression case2 =
        new CaseWhenThenExpression(whenThenExpressionList, intValue("99999"));
    assertSerializeDeserializeEqual(case2);

    // without ELSE:
    CaseWhenThenExpression case3 = new CaseWhenThenExpression(whenThenExpressionList, null);
    assertSerializeDeserializeEqual(case3);
  }
}
