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

package org.apache.iotdb.db.query.udf.example.relational.iotdblocal;

import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

/** Exercises IoTDBLocal log APIs at each scalar-function lifecycle hook. */
public class IoTDBLocalLogFunction implements ScalarFunction {

  private boolean evaluateLogged;
  private boolean destroyLogged;

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.STRING).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments, IoTDBLocal local) {
    IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.SCALAR_BEFORE_START);
  }

  @Override
  public Object evaluate(Record input) {
    return new Binary("ok", TSFileConfig.STRING_CHARSET);
  }

  @Override
  public Object evaluate(Record input, IoTDBLocal local) {
    if (!evaluateLogged) {
      evaluateLogged = true;
      IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.SCALAR_EVALUATE);
    }
    return evaluate(input);
  }

  @Override
  public void beforeDestroy(IoTDBLocal local) {
    if (!destroyLogged) {
      destroyLogged = true;
      IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.SCALAR_BEFORE_DESTROY);
    }
  }
}
