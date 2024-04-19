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

package org.apache.iotdb.db.queryengine.transformation.dag.udf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import org.apache.tsfile.enums.TSDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDFParametersFactory {

  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();

  public static final String TIMESTAMP_PRECISION = "timestampPrecision";

  public static UDFParameters buildUdfParameters(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    return new UDFParameters(
        childExpressions,
        UDFDataTypeTransformer.transformToUDFDataTypeList(childExpressionDataTypes),
        attributes,
        buildSystemAttributes());
  }

  private static Map<String, String> buildSystemAttributes() {
    Map<String, String> systemAttributes = new HashMap<>();
    systemAttributes.put(TIMESTAMP_PRECISION, CONFIG.getTimestampPrecision());
    return systemAttributes;
  }
}
