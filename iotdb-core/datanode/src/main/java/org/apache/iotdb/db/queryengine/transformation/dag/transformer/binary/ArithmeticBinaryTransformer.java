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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils.castValueToDouble;

public abstract class ArithmeticBinaryTransformer extends BinaryTransformer {

  protected ArithmeticBinaryTransformer(LayerReader leftReader, LayerReader rightReader) {
    super(leftReader, rightReader);
  }

  @Override
  protected void checkType() {
    if (leftReaderDataType == TSDataType.BOOLEAN || rightReaderDataType == TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException(TSDataType.BOOLEAN.name());
    }
    if (leftReaderDataType == TSDataType.TEXT || rightReaderDataType == TSDataType.TEXT) {
      throw new UnSupportedDataTypeException(TSDataType.TEXT.name());
    }
  }

  @Override
  protected void transformAndCache(
      Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
      throws QueryProcessException {
    double leftValue = castValueToDouble(leftValues, leftReaderDataType, leftIndex);
    double rightValue = castValueToDouble(rightValues, rightReaderDataType, rightIndex);
    builder.writeDouble(evaluate(leftValue, rightValue));
  }

  protected abstract double evaluate(double leftOperand, double rightOperand);

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.DOUBLE};
  }
}
