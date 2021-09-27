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

import org.apache.iotdb.influxdb.IoTDBInfluxDBUtils;
import org.apache.iotdb.influxdb.query.expression.Expression;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.List;

public class SpreadFunction extends Aggregate {
  private Double maxNum = null;
  private Double minNum = null;

  public SpreadFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public SpreadFunction(List<Expression> expressionList, Session session, String path) {
    super(expressionList, session, path);
  }

  @Override
  public void updateValue(FunctionValue functionValue) {
    Object value = functionValue.getValue();
    if (!(value instanceof Number)) {
      throw new IllegalArgumentException("not support this type");
    }

    double tmpValue = ((Number) value).doubleValue();
    if (maxNum == null || tmpValue > maxNum) {
      maxNum = tmpValue;
    }
    if (minNum == null || tmpValue < minNum) {
      minNum = tmpValue;
    }
  }

  @Override
  public FunctionValue calculate() {
    return new FunctionValue(maxNum - minNum, 0L);
  }

  @Override
  public FunctionValue calculateByIoTDBFunc() {
    Double maxNumber = null;
    Double minNumber = null;
    try {
      SessionDataSet sessionDataSet =
          this.session.executeQueryStatement(
              IoTDBInfluxDBUtils.generateFunctionSql("max_value", getParmaName(), path));
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
        Object o = IoTDBInfluxDBUtils.iotdbFiledCvt(fields.get(1));
        if ((o instanceof Number)) {
          double tmpValue = ((Number) o).doubleValue();
          if (maxNumber == null) {
            maxNumber = tmpValue;
          } else if (tmpValue > maxNumber) {
            maxNumber = tmpValue;
          }
        }
      }
      sessionDataSet =
          this.session.executeQueryStatement(
              IoTDBInfluxDBUtils.generateFunctionSql("min_value", getParmaName(), path));
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
        Object o = IoTDBInfluxDBUtils.iotdbFiledCvt(fields.get(1));
        if ((o instanceof Number)) {
          double tmpValue = ((Number) o).doubleValue();
          if (minNumber == null) {
            minNumber = tmpValue;
          } else if (tmpValue < minNumber) {
            minNumber = tmpValue;
          }
        }
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    if (maxNumber == null || minNumber == null) {
      return new FunctionValue(null, null);
    }
    return new FunctionValue(maxNumber - minNumber, 0L);
  }
}
