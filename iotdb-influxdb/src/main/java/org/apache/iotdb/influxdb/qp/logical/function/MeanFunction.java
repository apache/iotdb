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

import org.apache.iotdb.influxdb.IotDBInfluxDBUtils;
import org.apache.iotdb.influxdb.qp.utils.MathUtil;
import org.apache.iotdb.influxdb.qp.utils.TypeUtil;
import org.apache.iotdb.influxdb.query.expression.Expression;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.List;

public class MeanFunction extends Aggregate {
  public List<Double> numbers = new ArrayList<>();

  public MeanFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public MeanFunction(List<Expression> expressionList, Session session, String path) {
    super(expressionList, session, path);
  }

  public MeanFunction() {}

  @Override
  public void updateValue(FunctionValue functionValue) {
    Object value = functionValue.getValue();
    if (TypeUtil.checkDecimal(value)) {
      numbers.add(((Number) value).doubleValue());
    } else {
      throw new IllegalArgumentException("mean not valid type");
    }
  }

  @Override
  public FunctionValue calculate() {
    return new FunctionValue(
        numbers.size() == 0 ? numbers : String.valueOf(MathUtil.Mean(numbers)), 0L);
  }

  @Override
  public FunctionValue calculateByIotdbFunc() {

    int sum = 0;
    int count = 0;
    try {
      SessionDataSet sessionDataSet =
          this.session.executeQueryStatement(
              IotDBInfluxDBUtils.generateFunctionSql("count", getParmaName(), path));
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
        if (fields.get(1).getDataType() != null) {
          count += fields.get(1).getLongV();
        }
      }

      sessionDataSet =
          this.session.executeQueryStatement(
              IotDBInfluxDBUtils.generateFunctionSql("sum", getParmaName(), path));
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
        if (fields.get(1).getDataType() != null) {
          sum += fields.get(1).getDoubleV();
        }
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return new FunctionValue(count != 0 ? sum / count : null, count != 0 ? 0L : null);
  }
}
