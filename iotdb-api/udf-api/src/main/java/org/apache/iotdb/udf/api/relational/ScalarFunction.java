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

import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

public interface ScalarFunction extends SQLFunction {

  /**
   * This method is used to validate {@link FunctionParameters}.
   *
   * @param parameters parameters used to validate
   * @throws Exception if any parameter is not valid
   */
  void validate(FunctionParameters parameters) throws Exception;

  /**
   * This method is used to infer the output data type of the transformation.
   *
   * @param parameters input parameters
   * @return the output data type
   */
  Type inferOutputType(FunctionParameters parameters);

  /**
   * This method will be called to process the transformation. In a single UDF query, this method
   * may be called multiple times.
   *
   * @param input original input data row
   * @throws Exception the user can throw errors if necessary
   * @throws UnsupportedOperationException if the user does not override this method
   */
  Object evaluate(Record input) throws Exception;

  /** This method is mainly used to release the resources used in the SQLFunction. */
  default void beforeDestroy() {
    // do nothing
  }
}
