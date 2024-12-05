/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.udf;

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class UDAFInformationInferrer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDAFInformationInferrer.class);

  protected final String functionName;

  public UDAFInformationInferrer(String functionName) {
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

  private UDAFConfigurations reflectAndGetConfigurations(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes)
      throws Exception {
    UDAF udaf = UDFManagementService.getInstance().reflect(functionName, UDAF.class);

    UDFParameters parameters =
        UDFParametersFactory.buildUdfParameters(
            childExpressions, childExpressionDataTypes, attributes);
    udaf.validate(new UDFParameterValidator(parameters));

    // currently UDAF configuration does not need Zone ID
    UDAFConfigurations configurations = new UDAFConfigurations();
    udaf.beforeStart(parameters, configurations);
    udaf.beforeDestroy();
    return configurations;
  }
}
