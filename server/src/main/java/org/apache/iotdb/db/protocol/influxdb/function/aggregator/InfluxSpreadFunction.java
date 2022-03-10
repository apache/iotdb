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

package org.apache.iotdb.db.protocol.influxdb.function.aggregator;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.util.FieldUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class InfluxSpreadFunction extends InfluxAggregator {
  private Double maxNum = null;
  private Double minNum = null;

  public InfluxSpreadFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public InfluxSpreadFunction(
      List<Expression> expressionList, String path, ServiceProvider serviceProvider) {
    super(expressionList, path, serviceProvider);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    return new InfluxFunctionValue(maxNum - minNum, 0L);
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    Double maxValue = null;
    Double minValue = null;
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String functionSqlMaxValue =
          StringUtils.generateFunctionSql("max_value", getParmaName(), path);
      QueryPlan queryPlan =
          (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSqlMaxValue);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              functionSqlMaxValue,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, queryPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      while (queryDataSet.hasNext()) {
        List<Path> paths = queryDataSet.getPaths();
        List<Field> fields = queryDataSet.next().getFields();
        for (int i = 0; i < paths.size(); i++) {
          Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
          if (o instanceof Number) {
            double tmpValue = ((Number) o).doubleValue();
            if (maxValue == null) {
              maxValue = tmpValue;
            } else if (tmpValue > maxValue) {
              maxValue = tmpValue;
            }
          }
        }
      }

      String functionSqlMinValue =
          StringUtils.generateFunctionSql("min_value", getParmaName(), path);
      queryPlan =
          (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSqlMinValue);
      queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              functionSqlMinValue,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, queryPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      while (queryDataSet.hasNext()) {
        List<Path> paths = queryDataSet.getPaths();
        List<Field> fields = queryDataSet.next().getFields();
        for (int i = 0; i < paths.size(); i++) {
          Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
          if (o instanceof Number) {
            double tmpValue = ((Number) o).doubleValue();
            if (minValue == null) {
              minValue = tmpValue;
            } else if (tmpValue < minValue) {
              minValue = tmpValue;
            }
          }
        }
      }
    } catch (QueryProcessException
        | TException
        | StorageEngineException
        | SQLException
        | IOException
        | InterruptedException
        | QueryFilterOptimizationException
        | MetadataException e) {
      throw new InfluxDBException(e.getMessage());
    } finally {
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
    if (maxValue == null || minValue == null) {
      return new InfluxFunctionValue(null, null);
    }
    return new InfluxFunctionValue(maxValue - minValue, 0L);
  }

  @Override
  public void updateValue(InfluxFunctionValue functionValue) {
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
}
