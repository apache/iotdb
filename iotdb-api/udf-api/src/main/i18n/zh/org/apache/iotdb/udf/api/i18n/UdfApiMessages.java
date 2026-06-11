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
  public static final String UNSUPPORTED_VALUE_TYPE = "不支持的值类型。";
  public static final String UNKNOWN_TYPE = "未知类型：";

  // Argument
  public static final String UNKNOWN_ARGUMENT_TYPE = "未知参数类型：";

  // TableFunctionProcessorProvider
  public static final String TABLE_FUNCTION_DOES_NOT_PROCESS_INPUT_DATA =
      "此表函数不处理输入数据";
  public static final String TABLE_FUNCTION_DOES_NOT_PROCESS_LEAF_DATA =
      "此表函数不处理叶子数据";

  // DescribedSchema
  public static final String DESCRIBED_SCHEMA_HAS_NO_FIELDS = "DescribedSchema 没有字段";

  // ScalarArgument
  public static final String UNKNOWN_SCALAR_ARG_TYPE = "未知类型：";

  // TableArgument
  public static final String FIELD_NAMES_AND_TYPES_MUST_HAVE_SAME_SIZE =
      "fieldNames 和 fieldTypes 必须具有相同的大小";

  // ParameterSpecification
  public static final String NON_NULL_DEFAULT_VALUE_FOR_REQUIRED_ARG =
      "必填参数不能有非 null 默认值";

  // ScalarFunctionAnalysis
  public static final String SCALAR_FUNCTION_ANALYSIS_OUTPUT_DATA_TYPE_NOT_SET =
      "ScalarFunctionAnalysis 的 outputDataType 未设置。";

  // AggregateFunctionAnalysis
  public static final String AGGREGATE_FUNCTION_ANALYSIS_OUTPUT_DATA_TYPE_NOT_SET =
      "AggregateFunctionAnalysis 的 outputDataType 未设置。";

  // UDTFConfigurations
  public static final String ACCESS_STRATEGY_NOT_SET = "访问策略未设置。";

  // UDFConfigurations
  public static final String UDF_OUTPUT_DATA_TYPE_NOT_SET = "UDF 的 outputDataType 未设置。";

  // SlidingTimeWindowAccessStrategy
  public static final String METHOD_DEPRECATED_SINCE_V014 = "该方法自 v0.14 起已废弃。";

  // Type
  public static final String UNSUPPORTED_TYPE = "不支持的类型：";

  // RowImpl
  public static final String INDEX_OUT_OF_BOUND = "索引越界错误！";
  public static final String INVALID_INPUT = "无效输入：";

  private UdfApiMessages() {}
}
