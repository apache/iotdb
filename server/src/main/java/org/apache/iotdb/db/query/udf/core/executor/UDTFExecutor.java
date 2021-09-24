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

package org.apache.iotdb.db.query.udf.core.executor;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.time.ZoneId;
import java.util.Map;

public class UDTFExecutor {

  protected final FunctionExpression expression;
  protected final UDTFConfigurations configurations;

  protected UDTF udtf;
  protected ElasticSerializableTVList collector;

  public UDTFExecutor(FunctionExpression expression, ZoneId zoneId) {
    this.expression = expression;
    configurations = new UDTFConfigurations(zoneId);
  }

  public void beforeStart(
      long queryId,
      float collectorMemoryBudgetInMB,
      Map<Expression, TSDataType> expressionDataTypeMap)
      throws QueryProcessException {
    udtf = (UDTF) UDFRegistrationService.getInstance().reflect(expression);

    UDFParameters parameters = new UDFParameters(expression, expressionDataTypeMap);

    try {
      udtf.validate(new UDFParameterValidator(parameters));
    } catch (Exception e) {
      onError("validate(UDFParameterValidator)", e);
    }

    try {
      udtf.beforeStart(parameters, configurations);
    } catch (Exception e) {
      onError("beforeStart(UDFParameters, UDTFConfigurations)", e);
    }
    configurations.check();

    collector =
        ElasticSerializableTVList.newElasticSerializableTVList(
            configurations.getOutputDataType(), queryId, collectorMemoryBudgetInMB, 1);
  }

  public void execute(Row row) throws QueryProcessException {
    try {
      udtf.transform(row, collector);
    } catch (Exception e) {
      onError("transform(Row, PointCollector)", e);
    }
  }

  public void execute(RowWindow rowWindow) throws QueryProcessException {
    try {
      udtf.transform(rowWindow, collector);
    } catch (Exception e) {
      onError("transform(RowWindow, PointCollector)", e);
    }
  }

  public void terminate() throws QueryProcessException {
    try {
      udtf.terminate(collector);
    } catch (Exception e) {
      onError("terminate(PointCollector)", e);
    }
  }

  public void beforeDestroy() {
    udtf.beforeDestroy();
  }

  private void onError(String methodName, Exception e) throws QueryProcessException {
    throw new QueryProcessException(
        String.format(
                "Error occurred during executing UDTF#%s: %s", methodName, System.lineSeparator())
            + e);
  }

  public FunctionExpression getExpression() {
    return expression;
  }

  public UDTFConfigurations getConfigurations() {
    return configurations;
  }

  public ElasticSerializableTVList getCollector() {
    return collector;
  }
}
