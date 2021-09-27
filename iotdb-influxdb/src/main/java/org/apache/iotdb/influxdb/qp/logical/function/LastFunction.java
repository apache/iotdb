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

public class LastFunction extends Selector {
  private Object value;

  public LastFunction(List<Expression> expressionList) {
    super(expressionList);
    this.setTimestamp(Long.MIN_VALUE);
  }

  public LastFunction(List<Expression> expressionList, Session session, String path) {
    super(expressionList, session, path);
  }

  public LastFunction() {}

  @Override
  public void updateValueAndRelate(FunctionValue functionValue, List<Object> values) {

    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (timestamp >= this.getTimestamp()) {
      this.value = value;
      this.setTimestamp(timestamp);
      this.setRelatedValues(values);
    }
  }

  @Override
  public FunctionValue calculate() {
    return new FunctionValue(value, this.getTimestamp());
  }

  @Override
  public FunctionValue calculateByIoTDBFunc() {
    Object LastValue = null;
    Long LastTime = null;
    try {
      SessionDataSet sessionDataSet =
          this.session.executeQueryStatement(
              IoTDBInfluxDBUtils.generateFunctionSql("last_value", getParmaName(), path));
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
        Object o = IoTDBInfluxDBUtils.iotdbFiledCvt(fields.get(1));
        if (o != null) {
          String newPath =
              String.format(
                  "select %s from %s where %s.%s=%s",
                  getParmaName(), fields.get(0), fields.get(0), getParmaName(), o);
          SessionDataSet sessionDataSetNew = this.session.executeQueryStatement(newPath);
          while (sessionDataSetNew.hasNext()) {
            RowRecord recordNew = sessionDataSetNew.next();
            List<org.apache.iotdb.tsfile.read.common.Field> newFields = recordNew.getFields();
            Long time = recordNew.getTimestamp();
            if (LastValue == null && LastTime == null) {
              LastValue = IoTDBInfluxDBUtils.iotdbFiledCvt(newFields.get(0));
              LastTime = time;
            } else {
              if (time > LastTime) {
                LastValue = IoTDBInfluxDBUtils.iotdbFiledCvt(newFields.get(0));
                LastTime = time;
              }
            }
          }
        }
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    if (LastTime == null || LastValue == null) {
      return new FunctionValue(null, null);
    }
    return new FunctionValue(LastValue, LastTime);
  }
}
