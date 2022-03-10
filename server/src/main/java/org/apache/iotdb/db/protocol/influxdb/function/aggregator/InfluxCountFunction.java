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
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.InfluxDBUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class InfluxCountFunction extends InfluxAggregator {
  private int countNum = 0;

  public InfluxCountFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public InfluxCountFunction(
      List<Expression> expressionList, String path, ServiceProvider serviceProvider) {
    super(expressionList, path, serviceProvider);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    return new InfluxFunctionValue(this.countNum, 0L);
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    long count = 0;
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String functionSql = InfluxDBUtils.generateFunctionSql("count", getParmaName(), path);
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
        List<Field> fields = queryDataSet.next().getFields();
        for (Field field : fields) {
          count += field.getLongV();
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
    return new InfluxFunctionValue(count, 0L);
  }

  @Override
  public void updateValue(InfluxFunctionValue functionValue) {
    this.countNum++;
  }
}
