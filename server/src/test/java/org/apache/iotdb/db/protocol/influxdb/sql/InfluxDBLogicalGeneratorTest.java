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
package org.apache.iotdb.db.protocol.influxdb.sql;

import org.apache.iotdb.db.protocol.influxdb.expression.ResultColumn;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.NodeExpression;
import org.apache.iotdb.db.protocol.influxdb.operator.*;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class InfluxDBLogicalGeneratorTest {
  @Test
  public void testParserSql1() {
    QueryOperator operator =
        (QueryOperator) InfluxDBLogicalGenerator.generate("SELECT * FROM h2o_feet");
    List<ResultColumn> resultColumnList = operator.getSelectComponent().getInfluxResultColumns();
    assertEquals(resultColumnList.size(), 1);
    NodeExpression nodeExpression = (NodeExpression) resultColumnList.get(0).getExpression();
    assertEquals(nodeExpression.getName(), "*");
    assertEquals(operator.getFromComponent().getNodeName().get(0), "h2o_feet");
    assertNull(operator.getWhereComponent());
  }

  @Test
  public void testParserSql2() {
    QueryOperator operator =
        (QueryOperator)
            InfluxDBLogicalGenerator.generate("SELECT a,b,c FROM h2o_feet where a>1 and b<1");
    List<ResultColumn> resultColumnList = operator.getSelectComponent().getInfluxResultColumns();
    assertEquals(resultColumnList.size(), 3);
    NodeExpression nodeExpression = (NodeExpression) resultColumnList.get(0).getExpression();
    assertEquals(nodeExpression.getName(), "a");
    WhereComponent whereComponent = operator.getWhereComponent();
    FilterOperator filterOperator = (FilterOperator) whereComponent.getFilterOperator();
    assertEquals(filterOperator.getFilterType().toString(), "KW_AND");
    assertEquals(filterOperator.getChildren().size(), 2);
    BasicFunctionOperator basicFunctionOperator =
        (BasicFunctionOperator) filterOperator.getChildren().get(0);
    assertEquals(basicFunctionOperator.getValue(), "1");
    assertEquals(basicFunctionOperator.getKeyName(), "a");
    assertNull(basicFunctionOperator.getFilterType());
  }
}
