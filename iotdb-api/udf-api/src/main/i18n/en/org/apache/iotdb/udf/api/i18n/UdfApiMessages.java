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

package org.apache.iotdb.udf.api.i18n;

public final class UdfApiMessages {

  // MapTableFunctionHandle
  public static final String UNSUPPORTED_VALUE_TYPE = "Unsupported value type.";
  public static final String UNKNOWN_TYPE = "Unknown type: ";

  // Argument
  public static final String UNKNOWN_ARGUMENT_TYPE = "Unknown argument type: ";

  // TableFunctionProcessorProvider
  public static final String TABLE_FUNCTION_DOES_NOT_PROCESS_INPUT_DATA =
      "this table function does not process input data";
  public static final String TABLE_FUNCTION_DOES_NOT_PROCESS_LEAF_DATA =
      "this table function does not process leaf data";

  // DescribedSchema
  public static final String DESCRIBED_SCHEMA_HAS_NO_FIELDS = "DescribedSchema has no fields";

  // ScalarArgument
  public static final String UNKNOWN_SCALAR_ARG_TYPE = "Unknown type: ";

  // TableArgument
  public static final String FIELD_NAMES_AND_TYPES_MUST_HAVE_SAME_SIZE =
      "fieldNames and fieldTypes must have the same size";

  // ParameterSpecification
  public static final String NON_NULL_DEFAULT_VALUE_FOR_REQUIRED_ARG =
      "non-null default value for a required argument";

  // ScalarFunctionAnalysis
  public static final String SCALAR_FUNCTION_ANALYSIS_OUTPUT_DATA_TYPE_NOT_SET =
      "ScalarFunctionAnalysis outputDataType is not set.";

  // AggregateFunctionAnalysis
  public static final String AGGREGATE_FUNCTION_ANALYSIS_OUTPUT_DATA_TYPE_NOT_SET =
      "AggregateFunctionAnalysis outputDataType is not set.";

  // UDTFConfigurations
  public static final String ACCESS_STRATEGY_NOT_SET = "Access strategy is not set.";

  // UDFConfigurations
  public static final String UDF_OUTPUT_DATA_TYPE_NOT_SET = "UDF outputDataType is not set.";

  // SlidingTimeWindowAccessStrategy
  public static final String METHOD_DEPRECATED_SINCE_V014 =
      "The method is deprecated since v0.14.";

  // Type
  public static final String UNSUPPORTED_TYPE = "Unsupported type: ";

  // RowImpl
  public static final String INDEX_OUT_OF_BOUND = "Index out of bound error!";
  public static final String INVALID_INPUT = "Invalid input: ";

  private UdfApiMessages() {}
}
