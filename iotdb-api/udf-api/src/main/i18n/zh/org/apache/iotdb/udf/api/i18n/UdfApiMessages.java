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

  // ScalarParameterSpecification
  public static final String
      EXCEPTION_DEFAULT_VALUE_ARG_DOES_NOT_MATCH_THE_DECLARED_TYPE_ARG_76648C9D =
          "默认值 %s 与声明的类型不匹配：%s";

  // SessionTimeWindowAccessStrategy
  public static final String
      EXCEPTION_PARAMETER_SESSIONTIMEGAP_ARG_SHOULD_BE_EQUAL_TO_OR_GREATER_THAN_ZERO_20C0672D =
          "参数 sessionTimeGap(%d) 应当大于等于 0。";
  public static final String EXCEPTION_DISPLAYWINDOWEND_ARG_DISPLAYWINDOWBEGIN_ARG_216864F1 =
      "displayWindowEnd(%d) < displayWindowBegin(%d)";

  // SlidingTimeWindowAccessStrategy
  public static final String EXCEPTION_PARAMETER_TIMEINTERVAL_ARG_SHOULD_BE_POSITIVE_7CF8DCE4 =
      "参数 timeInterval(%d) 必须为正数。";
  public static final String EXCEPTION_PARAMETER_SLIDINGSTEP_ARG_SHOULD_BE_POSITIVE_BBB66A4C =
      "参数 slidingStep(%d) 必须为正数。";

  // SlidingSizeWindowAccessStrategy
  public static final String EXCEPTION_PARAMETER_WINDOWSIZE_ARG_SHOULD_BE_POSITIVE_7170E783 =
      "参数 windowSize(%d) 必须为正数。";

  // StateWindowAccessStrategy
  public static final String
      EXCEPTION_PARAMETER_DELTA_ARG_SHOULD_BE_POSITIVE_OR_EQUAL_TO_0_787DD7AE =
          "参数 delta(%f) 必须为正数或等于 0。";

  // UDFInputSeriesNumberNotValidException
  public static final String
      EXCEPTION_THE_NUMBER_OF_THE_INPUT_SERIES_IS_NOT_VALID_EXPECTED_ARG_ACTUAL_ARG_48AF79C9 =
          "输入序列的数量无效。期望：%d。实际：%d。";
  public static final String
      EXCEPTION_THE_NUMBER_OF_THE_INPUT_SERIES_IS_NOT_VALID_EXPECTED_ARG_ARG_ACTUAL_ARG_819C0F0A =
          "输入序列的数量无效。期望：[%d, %d]。实际：%d。";

  // UDFInputSeriesIndexNotValidException
  public static final String
      EXCEPTION_THE_INDEX_ARG_OF_THE_INPUT_SERIES_IS_NOT_VALID_VALID_INDEX_RANGE_0_ARG_8B8367FE =
          "输入序列的索引 (%d) 无效。有效索引范围：[0, %d)。";

  // UDFAttributeNotProvidedException
  public static final String EXCEPTION_ATTRIBUTE_ARG_IS_REQUIRED_BUT_WAS_NOT_PROVIDED_867D4638 =
      "属性 \"%s\" 是必填的，但未提供。";

  // UDFOutputSeriesDataTypeNotValidException
  public static final String
      EXCEPTION_THE_DATA_TYPE_OF_THE_OUTPUT_SERIES_INDEX_ARG_IS_NOT_VALID_EXPECTED_ARG_388E46F3 =
          "输出序列（索引：%d）的数据类型无效。期望：%s。";

  // UDFInputSeriesDataTypeNotValidException
  public static final String
      EXCEPTION_THE_DATA_TYPE_OF_THE_INPUT_SERIES_INDEX_ARG_IS_NOT_VALID_EXPECTED_ARG_ACTUAL_ARG_6DE799E5 =
          "输入序列（索引：%d）的数据类型无效。期望：%s。实际：%s。";

  private UdfApiMessages() {}
}
