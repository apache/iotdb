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

import org.apache.iotdb.db.protocol.influxdb.expression.InfluxResultColumn;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.InfluxNodeExpression;
import org.apache.iotdb.db.protocol.influxdb.operator.*;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class InfluxDBLogicalGeneratorTest {
  @Test
  public void testParserSql1() {
    InfluxQueryOperator operator =
        (InfluxQueryOperator) InfluxDBLogicalGenerator.generate("SELECT * FROM h2o_feet");
    List<InfluxResultColumn> influxResultColumnList =
        operator.getSelectComponent().getInfluxResultColumns();
    assertEquals(influxResultColumnList.size(), 1);
    InfluxNodeExpression influxNodeExpression =
        (InfluxNodeExpression) influxResultColumnList.get(0).getExpression();
    assertEquals(influxNodeExpression.getName(), "*");
    assertEquals(operator.getFromComponent().getNodeName().get(0), "h2o_feet");
    assertNull(operator.getWhereComponent());
  }

  @Test
  public void testParserSql2() {
    InfluxQueryOperator operator =
        (InfluxQueryOperator)
            InfluxDBLogicalGenerator.generate("SELECT a,b,c FROM h2o_feet where a>1 and b<1");
    List<InfluxResultColumn> influxResultColumnList =
        operator.getSelectComponent().getInfluxResultColumns();
    assertEquals(influxResultColumnList.size(), 3);
    InfluxNodeExpression influxNodeExpression =
        (InfluxNodeExpression) influxResultColumnList.get(0).getExpression();
    assertEquals(influxNodeExpression.getName(), "a");
    InfluxWhereComponent influxWhereComponent = operator.getWhereComponent();
    InfluxFilterOperator filterOperator =
        (InfluxFilterOperator) influxWhereComponent.getFilterOperator();
    assertEquals(filterOperator.getFilterType().toString(), "KW_AND");
    assertEquals(filterOperator.getChildren().size(), 2);
    InfluxBasicFunctionOperatorInflux basicFunctionOperator =
        (InfluxBasicFunctionOperatorInflux) filterOperator.getChildren().get(0);
    assertEquals(basicFunctionOperator.getValue(), "1");
    assertEquals(basicFunctionOperator.getKeyName(), "a");
    assertNull(basicFunctionOperator.getFilterType());
  }
}
