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

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class UDTFInformationInferrer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDTFInformationInferrer.class);

  protected final String functionName;

  public UDTFInformationInferrer(String functionName) {
    this.functionName = functionName;
  }

  public TSDataType inferOutputType(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    try {
      return UDFDataTypeTransformer.transformToTsDataType(
          reflectAndGetConfigurations(childExpressions, childExpressionDataTypes, attributes)
              .getOutputDataType());
    } catch (Exception e) {
      LOGGER.warn("Error occurred during inferring UDF data type", e);
      throw new SemanticException(
          String.format("Error occurred during inferring UDF data type: %s", System.lineSeparator())
              + e);
    }
  }

  public AccessStrategy getAccessStrategy(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    try {

      return reflectAndGetConfigurations(childExpressions, childExpressionDataTypes, attributes)
          .getAccessStrategy();
    } catch (Exception e) {
      LOGGER.warn("Error occurred during getting UDF access strategy", e);
      throw new SemanticException(
          String.format(
                  "Error occurred during getting UDF access strategy: %s", System.lineSeparator())
              + e);
    }
  }

  private UDTFConfigurations reflectAndGetConfigurations(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes)
      throws Exception {
    UDTF udtf = (UDTF) UDFManagementService.getInstance().reflect(functionName);

    UDFParameters parameters =
        new UDFParameters(
            childExpressions,
            UDFDataTypeTransformer.transformToUDFDataTypeList(childExpressionDataTypes),
            attributes);
    udtf.validate(new UDFParameterValidator(parameters));

    // use ZoneId.systemDefault() because UDF's data type is ZoneId independent
    UDTFConfigurations configurations = new UDTFConfigurations(ZoneId.systemDefault());
    udtf.beforeStart(parameters, configurations);
    udtf.beforeDestroy();
    return configurations;
  }
}
