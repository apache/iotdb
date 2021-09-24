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

package org.apache.iotdb.influxdb.qp.logical.function;

import org.apache.iotdb.influxdb.qp.constant.SQLConstant;
import org.apache.iotdb.influxdb.query.expression.Expression;
import org.apache.iotdb.session.Session;

import java.util.List;

public class FunctionFactory {
  public static Function generateFunction(String name, List<Expression> expressionList) {
    switch (name) {
      case SQLConstant.MAX:
        return new MaxFunction(expressionList);
      case SQLConstant.MIN:
        return new MinFunction(expressionList);
      case SQLConstant.MEAN:
        return new MeanFunction(expressionList);
      case SQLConstant.LAST:
        return new LastFunction(expressionList);
      case SQLConstant.FIRST:
        return new FirstFunction(expressionList);
      case SQLConstant.COUNT:
        return new CountFunction(expressionList);
      case SQLConstant.MEDIAN:
        return new MedianFunction(expressionList);
      case SQLConstant.MODE:
        return new ModeFunction(expressionList);
      case SQLConstant.SPREAD:
        return new SpreadFunction(expressionList);
      case SQLConstant.STDDEV:
        return new StddevFunction(expressionList);
      case SQLConstant.SUM:
        return new SumFunction(expressionList);
      default:
        throw new IllegalArgumentException("not support aggregation name:" + name);
    }
  }

  public static Function generateFunctionBySession(
      String name, List<Expression> expressionList, Session session, String path) {
    switch (name) {
      case SQLConstant.MAX:
        return new MaxFunction(expressionList, session, path);
      case SQLConstant.MIN:
        return new MinFunction(expressionList, session, path);
      case SQLConstant.MEAN:
        return new MeanFunction(expressionList, session, path);
      case SQLConstant.LAST:
        return new LastFunction(expressionList, session, path);
      case SQLConstant.FIRST:
        return new FirstFunction(expressionList, session, path);
      case SQLConstant.COUNT:
        return new CountFunction(expressionList, session, path);
      case SQLConstant.SUM:
        return new SumFunction(expressionList, session, path);
      case SQLConstant.SPREAD:
        return new SpreadFunction(expressionList, session, path);
      default:
        throw new IllegalArgumentException("not support aggregation name:" + name);
    }
  }
}
