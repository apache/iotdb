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

package org.apache.iotdb.db.queryengine.transformation.dag.input;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;

public class ConstantInputReader implements LayerReader {
  private final TSDataType dataType;

  private final Column[] cachedColumns;

  public ConstantInputReader(ConstantOperand expression) throws QueryProcessException {
    if (expression == null) {
      throw new QueryProcessException(DataNodeQueryMessages.THE_EXPRESSION_CANNOT_BE_NULL);
    }

    Object value = CommonUtils.parseValue(expression.getDataType(), expression.getValueString());
    if (value == null) {
      throw new QueryProcessException(
          DataNodeQueryMessages.UNSUPPORTED_CONSTANT_OPERAND + expression.getExpressionString());
    }

    // Use RLEColumn to mimic column filled with same values
    dataType = expression.getDataType();
    cachedColumns = new Column[1];
    int count = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    try {
      cachedColumns[0] =
          new RunLengthEncodedColumn(
              TypeServices.CONSTANT_COLUMN_BUILDER_SERVICE
                  .call(Type.fromTsDataType(dataType))
                  .apply(value),
              count);
    } catch (final UnSupportedDataTypeException e) {
      throw new QueryProcessException(DataNodeQueryMessages.UNSUPPORTED_TYPE + dataType);
    }
  }

  @Override
  public boolean isConstantPointReader() {
    return true;
  }

  @Override
  public void consumedAll() {
    // Do nothing
  }

  @Override
  public Column[] current() throws IOException {
    return cachedColumns;
  }

  @Override
  public YieldableState yield() {
    return YieldableState.YIELDABLE;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {dataType};
  }
}
