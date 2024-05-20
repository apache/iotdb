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

import java.io.IOException;

public abstract class CompareBinaryTransformer extends BinaryTransformer {

  @FunctionalInterface
  protected interface Evaluator {

    boolean evaluate(Column leftValues, int leftIndex, Column rightValues, int rightIndex)
        throws QueryProcessException, IOException;
  }

  protected final Evaluator evaluator;

  protected CompareBinaryTransformer(LayerReader leftReader, LayerReader rightReader)
      throws UnSupportedDataTypeException {
    super(leftReader, rightReader);
    evaluator =
        TSDataType.TEXT.equals(leftReaderDataType)
            ? constructTextEvaluator()
            : constructNumberEvaluator();
  }

  protected abstract Evaluator constructNumberEvaluator();

  protected abstract Evaluator constructTextEvaluator();

  @Override
  protected final void checkType() {
    if (leftReaderDataType.equals(rightReaderDataType)) {
      return;
    }

    if (leftReaderDataType.equals(TSDataType.BOOLEAN)
        || rightReaderDataType.equals(TSDataType.BOOLEAN)) {
      throw new UnSupportedDataTypeException(TSDataType.BOOLEAN.toString());
    }
    if (leftReaderDataType.equals(TSDataType.TEXT) || rightReaderDataType.equals(TSDataType.TEXT)) {
      throw new UnSupportedDataTypeException(TSDataType.TEXT.toString());
    }
  }

  @Override
  protected final void transformAndCache(
      Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
      throws QueryProcessException, IOException {
    builder.writeBoolean(evaluator.evaluate(leftValues, leftIndex, rightValues, rightIndex));
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }
}
