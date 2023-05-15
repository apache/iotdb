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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter.model;

import org.apache.iotdb.commons.udf.builtin.ModelInferenceFunction;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.LinkedHashMap;
import java.util.List;

public abstract class ModelInferenceDescriptor {

  protected final ModelInferenceFunction functionType;

  protected final String modelId;

  protected String modelPath;

  protected List<FunctionExpression> modelInferenceOutputExpressions;

  public ModelInferenceDescriptor(ModelInferenceFunction functionType, String modelId) {
    this.functionType = functionType;
    this.modelId = modelId;
  }

  public ModelInferenceFunction getFunctionType() {
    return functionType;
  }

  public String getModelId() {
    return modelId;
  }

  public List<FunctionExpression> getModelInferenceOutputExpressions() {
    return modelInferenceOutputExpressions;
  }

  public void setModelInferenceOutputExpressions(
      List<FunctionExpression> modelInferenceOutputExpressions) {
    this.modelInferenceOutputExpressions = modelInferenceOutputExpressions;
  }

  public abstract String getParametersString();

  public abstract LinkedHashMap<String, String> getOutputAttributes();
}
