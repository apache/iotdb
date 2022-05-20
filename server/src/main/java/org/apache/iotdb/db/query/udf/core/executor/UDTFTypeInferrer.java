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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

public class UDTFTypeInferrer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDTFTypeInferrer.class);

  protected final FunctionExpression expression;

  public UDTFTypeInferrer(FunctionExpression expression) {
    this.expression = expression;
  }

  public TSDataType inferOutputType(TypeProvider typeProvider) {
    try {
      UDTF udtf = (UDTF) UDFRegistrationService.getInstance().reflect(expression);

      UDFParameters parameters = new UDFParameters(expression, typeProvider);
      udtf.validate(new UDFParameterValidator(parameters));

      // use ZoneId.systemDefault() because UDF's data type is ZoneId independent
      UDTFConfigurations configurations = new UDTFConfigurations(ZoneId.systemDefault());
      udtf.beforeStart(parameters, configurations);

      udtf.beforeDestroy();

      return configurations.getOutputDataType();
    } catch (Exception e) {
      LOGGER.warn("Error occurred during inferring UDF data type", e);
      throw new SemanticException(
          String.format("Error occurred during inferring UDF data type: %s", System.lineSeparator())
              + e);
    }
  }
}
