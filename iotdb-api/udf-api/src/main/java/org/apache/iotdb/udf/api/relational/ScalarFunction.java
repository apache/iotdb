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

package org.apache.iotdb.udf.api.relational;

import org.apache.iotdb.udf.api.customizer.config.ScalarFunctionConfig;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;

public interface ScalarFunction extends SQLFunction {

  /**
   * This method is used to validate {@linkplain FunctionParameters}.
   *
   * @param parameters parameters used to validate
   * @throws UDFException if any parameter is not valid
   */
  void validate(FunctionParameters parameters) throws UDFException;

  /**
   * This method is mainly used to initialize {@linkplain ScalarFunction} and set the output data
   * type. In this method, the user need to do the following things:
   *
   * <ul>
   *   <li>Use {@linkplain FunctionParameters} to get input data types and infer output data type.
   *   <li>Use {@linkplain FunctionParameters} to get necessary attributes.
   *   <li>Set the output data type in {@linkplain ScalarFunctionConfig}.
   * </ul>
   *
   * <p>This method is called after the ScalarFunction is instantiated and before the beginning of
   * the transformation process.
   *
   * @param parameters used to parse the input parameters entered by the user
   * @param configurations used to set the required properties in the ScalarFunction
   */
  void beforeStart(FunctionParameters parameters, ScalarFunctionConfig configurations);

  /**
   * This method will be called to process the transformation. In a single UDF query, this method
   * may be called multiple times.
   *
   * @param input original input data row
   * @throws UDFException the user can throw errors if necessary
   * @throws UnsupportedOperationException if the user does not override this method
   */
  Object evaluate(Record input) throws UDFException;

  /** This method is mainly used to release the resources used in the SQLFunction. */
  default void beforeDestroy() {
    // do nothing
  }
}
