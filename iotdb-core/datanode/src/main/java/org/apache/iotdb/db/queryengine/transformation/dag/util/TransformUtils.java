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

package org.apache.iotdb.db.queryengine.transformation.dag.util;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.ValueRecorder;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.util.Objects;

public class TransformUtils {

  private TransformUtils() {
    throw new IllegalStateException(
        DataNodeQueryMessages.TRANSFORMUTILS_SHOULD_NOT_BE_INSTANTIATED);
  }

  public static Column transformConstantOperandToColumn(ConstantOperand constantOperand) {
    Objects.requireNonNull(constantOperand);

    try {
      Object value =
          CommonUtils.parseValue(constantOperand.getDataType(), constantOperand.getValueString());
      if (value == null) {
        throw new UnsupportedOperationException(
            DataNodeQueryMessages.UNSUPPORTED_CONSTANT_OPERAND
                + constantOperand.getExpressionString());
      }

      return TypeServices.Transformation.CONSTANT_COLUMN_BUILDER_SERVICE
          .call(Type.fromTsDataType(constantOperand.getDataType()))
          .apply(value);
    } catch (QueryProcessException e) {
      throw new UnsupportedOperationException(e);
    }
  }

  public static boolean splitWindowForStateWindow(
      TSDataType dataType, ValueRecorder valueRecorder, double delta, Column values, int index) {
    return TypeServices.Transformation.STATE_WINDOW_SPLITTER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .split(valueRecorder, delta, values, index);
  }
}
