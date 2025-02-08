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

import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;

public interface ScalarFunction extends SQLFunction {
  /**
   * In this method, the user need to do the following things:
   *
   * <ul>
   *   <li>Validate {@linkplain FunctionArguments}. Throw {@link UDFArgumentNotValidException} if
   *       any parameter is not valid.
   *   <li>Use {@linkplain FunctionArguments} to get input data types and infer output data type.
   *   <li>Construct and return a {@linkplain ScalarFunctionAnalysis} object.
   * </ul>
   *
   * @param arguments arguments used to validate
   * @throws UDFArgumentNotValidException if any parameter is not valid
   * @return the analysis result of the scalar function
   */
  ScalarFunctionAnalysis analyze(FunctionArguments arguments) throws UDFArgumentNotValidException;

  /**
   * This method is called after the ScalarFunction is instantiated and before the beginning of the
   * transformation process. This method is mainly used to initialize the resources used in
   * ScalarFunction.
   *
   * @param arguments used to parse the input arguments entered by the user
   * @throws UDFException the user can throw errors if necessary
   */
  default void beforeStart(FunctionArguments arguments) throws UDFException {
    // do nothing
  }

  /**
   * This method will be called to process the transformation. In a single UDF query, this method
   * may be called multiple times.
   *
   * @param input original input data row
   * @throws UDFException the user can throw errors if necessary
   */
  Object evaluate(Record input) throws UDFException;

  /** This method is mainly used to release the resources used in the ScalarFunction. */
  default void beforeDestroy() {
    // do nothing
  }
}
