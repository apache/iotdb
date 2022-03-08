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

package org.apache.iotdb.db.protocol.influxdb.function.selector;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.InfluxDBUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class InfluxMaxFunction extends InfluxSelector {
  private Double doubleValue = Double.MIN_VALUE;
  private String stringValue = null;
  private boolean isNumber = false;
  private boolean isString = false;

  public InfluxMaxFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public InfluxMaxFunction(
      List<Expression> expressionList, String path, ServiceProvider serviceProvider) {
    super(expressionList, path, serviceProvider);
  }

  @Override
  public InfluxFunctionValue calculate() {
    if (!isString && !isNumber) {
      return new InfluxFunctionValue(null, null);
    } else if (isString) {
      return new InfluxFunctionValue(stringValue, this.getTimestamp());
    } else {
      return new InfluxFunctionValue(doubleValue, this.getTimestamp());
    }
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    Double maxNumber = null;
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String functionSql = InfluxDBUtils.generateFunctionSql("max_value", getParmaName(), path);
      QueryPlan queryPlan =
          (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSql);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              functionSql,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, queryPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      while (queryDataSet.hasNext()) {
        List<Path> paths = queryDataSet.getPaths();
        List<Field> fields = queryDataSet.next().getFields();
        for (int i = 0; i < paths.size(); i++) {
          Object o = InfluxDBUtils.iotdbFiledConvert(fields.get(i));
          if (o instanceof Number) {
            double tmpValue = ((Number) o).doubleValue();
            if (maxNumber == null) {
              maxNumber = tmpValue;
            } else if (tmpValue > maxNumber) {
              maxNumber = tmpValue;
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
    return new InfluxFunctionValue(maxNumber, maxNumber == null ? null : 0L);
  }

  @Override
  public void updateValueAndRelateValues(
      InfluxFunctionValue functionValue, List<Object> relatedValues) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (value instanceof Number) {
      if (!isNumber) {
        isNumber = true;
      }
      double tmpValue = ((Number) value).doubleValue();
      if (tmpValue >= this.doubleValue) {
        doubleValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(relatedValues);
      }
    } else if (value instanceof String) {
      String tmpValue = (String) value;
      if (!isString) {
        isString = true;
        stringValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(relatedValues);
      } else if (tmpValue.compareTo(this.stringValue) >= 0) {
        stringValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(relatedValues);
      }
    }
  }
}
