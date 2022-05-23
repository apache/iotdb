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

package org.apache.iotdb.db.mpp.transformation.dag.udf;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.api.UDTF;
import org.apache.iotdb.commons.udf.api.access.Row;
import org.apache.iotdb.commons.udf.api.access.RowWindow;
import org.apache.iotdb.commons.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.commons.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.commons.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class UDTFExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDTFExecutor.class);

  protected final String functionName;
  protected final UDTFConfigurations configurations;

  protected UDTF udtf;
  protected ElasticSerializableTVList collector;

  public UDTFExecutor(String functionName, ZoneId zoneId) {
    this.functionName = functionName;
    configurations = new UDTFConfigurations(zoneId);
  }

  public void beforeStart(
      long queryId,
      float collectorMemoryBudgetInMB,
      List<String> childExpressions,
      List<PartialPath> maybeTimeSeriesPaths,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    udtf = (UDTF) UDFRegistrationService.getInstance().reflect(functionName);

    final UDFParameters parameters =
        new UDFParameters(
            childExpressions, maybeTimeSeriesPaths, childExpressionDataTypes, attributes);

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

  public void execute(Row row, boolean isCurrentRowNull) {
    try {
      if (isCurrentRowNull) {
        // A null row will never trigger any UDF computing
        collector.putNull(row.getTime());
      } else {
        udtf.transform(row, collector);
      }
    } catch (Exception e) {
      onError("transform(Row, PointCollector)", e);
    }
  }

  public void execute(RowWindow rowWindow) {
    try {
      udtf.transform(rowWindow, collector);
    } catch (Exception e) {
      onError("transform(RowWindow, PointCollector)", e);
    }
  }

  public void terminate() {
    try {
      udtf.terminate(collector);
    } catch (Exception e) {
      onError("terminate(PointCollector)", e);
    }
  }

  public void beforeDestroy() {
    if (udtf != null) {
      udtf.beforeDestroy();
    }
  }

  private void onError(String methodName, Exception e) {
    LOGGER.warn("Error occurred during executing UDTF", e);
    throw new RuntimeException(
        String.format(
                "Error occurred during executing UDTF#%s: %s", methodName, System.lineSeparator())
            + e);
  }

  public UDTFConfigurations getConfigurations() {
    return configurations;
  }

  public ElasticSerializableTVList getCollector() {
    return collector;
  }
}
