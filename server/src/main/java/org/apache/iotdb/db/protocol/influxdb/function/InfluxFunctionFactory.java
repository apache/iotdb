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

import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxFirstFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxLastFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxMaxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxMinFunction;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.service.basic.ServiceProvider;

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
      //      case SQLConstant.MEAN:
      //        return new MeanFunction(expressionList);
      //      case SQLConstant.COUNT:
      //        return new CountFunction(expressionList);
      //      case SQLConstant.MEDIAN:
      //        return new MedianFunction(expressionList);
      //      case SQLConstant.MODE:
      //        return new ModeFunction(expressionList);
      //      case SQLConstant.SPREAD:
      //        return new SpreadFunction(expressionList);
      //      case SQLConstant.STDDEV:
      //        return new StddevFunction(expressionList);
      //      case SQLConstant.SUM:
      //        return new SumFunction(expressionList);
      default:
        throw new IllegalArgumentException("not support aggregation name:" + functionName);
    }
  }

  public static InfluxFunction generateFunctionByProvider(
      String functionName,
      List<Expression> expressionList,
      String path,
      ServiceProvider serviceProvider) {
    switch (functionName) {
      case InfluxSQLConstant.MAX:
        return new InfluxMaxFunction(expressionList, path, serviceProvider);
      case InfluxSQLConstant.MIN:
        return new InfluxMinFunction(expressionList, path, serviceProvider);
      case InfluxSQLConstant.FIRST:
        return new InfluxFirstFunction(expressionList, path, serviceProvider);
      case InfluxSQLConstant.LAST:
        return new InfluxLastFunction(expressionList, path, serviceProvider);
      //            case InfluxSQLConstant.MEAN:
      //                return new MeanFunction(expressionList, session, path);
      //            case InfluxSQLConstant.COUNT:
      //                return new CountFunction(expressionList, session, path);
      //            case InfluxSQLConstant.SUM:
      //                return new SumFunction(expressionList, session, path);
      //            case InfluxSQLConstant.SPREAD:
      //                return new SpreadFunction(expressionList, session, path);
      default:
        throw new IllegalArgumentException("not support aggregation name:" + functionName);
    }
  }
}
