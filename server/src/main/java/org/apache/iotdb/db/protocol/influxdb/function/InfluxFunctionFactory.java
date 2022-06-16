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
package org.apache.iotdb.db.protocol.influxdb.function;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxCountFunction;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxMeanFunction;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxMedianFunction;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxModeFunction;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxSpreadFunction;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxStddevFunction;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxSumFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxFirstFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxLastFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxMaxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxMinFunction;

import java.util.List;

public class InfluxFunctionFactory {

  public static InfluxFunction generateFunction(
      String functionName, List<Expression> expressionList) {
    switch (functionName) {
      case InfluxSQLConstant.MAX:
        return new InfluxMaxFunction(expressionList);
      case InfluxSQLConstant.MIN:
        return new InfluxMinFunction(expressionList);
      case InfluxSQLConstant.LAST:
        return new InfluxLastFunction(expressionList);
      case InfluxSQLConstant.FIRST:
        return new InfluxFirstFunction(expressionList);
      case InfluxSQLConstant.COUNT:
        return new InfluxCountFunction(expressionList);
      case InfluxSQLConstant.MEAN:
        return new InfluxMeanFunction(expressionList);
      case InfluxSQLConstant.MEDIAN:
        return new InfluxMedianFunction(expressionList);
      case InfluxSQLConstant.MODE:
        return new InfluxModeFunction(expressionList);
      case InfluxSQLConstant.SPREAD:
        return new InfluxSpreadFunction(expressionList);
      case InfluxSQLConstant.STDDEV:
        return new InfluxStddevFunction(expressionList);
      case InfluxSQLConstant.SUM:
        return new InfluxSumFunction(expressionList);
      default:
        throw new IllegalArgumentException("not support aggregation name:" + functionName);
    }
  }
}
