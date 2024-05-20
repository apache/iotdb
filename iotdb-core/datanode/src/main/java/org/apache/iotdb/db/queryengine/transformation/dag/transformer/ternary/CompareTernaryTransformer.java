/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.ternary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;

public abstract class CompareTernaryTransformer extends TernaryTransformer {

  @FunctionalInterface
  public interface Evaluator {

    boolean evaluate(
        Column firstValues,
        int firstIndex,
        Column secondValues,
        int secondIndex,
        Column thirdValues,
        int thirdIndex)
        throws QueryProcessException, IOException;
  }

  protected final Evaluator evaluator;

  protected CompareTernaryTransformer(
      LayerReader firstReader, LayerReader secondReader, LayerReader thirdReader)
      throws UnSupportedDataTypeException {
    super(firstReader, secondReader, thirdReader);
    evaluator =
        TSDataType.TEXT.equals(firstReaderDataType)
            ? constructTextEvaluator()
            : constructNumberEvaluator();
  }

  protected abstract Evaluator constructNumberEvaluator();

  protected abstract Evaluator constructTextEvaluator();

  @Override
  protected final void checkType() {
    if ((firstReaderDataType).equals(secondReaderDataType)
        && (firstReaderDataType).equals(thirdReaderDataType)) {
      return;
    }

    if (firstReaderDataType.equals(TSDataType.BOOLEAN)
        || secondReaderDataType.equals(TSDataType.BOOLEAN)
        || thirdReaderDataType.equals(TSDataType.BOOLEAN)) {
      throw new UnSupportedDataTypeException(TSDataType.BOOLEAN.toString());
    }

    if (firstReaderDataType.equals(TSDataType.TEXT)
        || secondReaderDataType.equals(TSDataType.TEXT)
        || thirdReaderDataType.equals(TSDataType.TEXT)) {
      throw new UnSupportedDataTypeException(TSDataType.TEXT.toString());
    }
  }

  @Override
  protected final void transformAndCache(
      Column firstValues,
      int firstIndex,
      Column secondValues,
      int secondIndex,
      Column thirdValues,
      int thirdIndex,
      ColumnBuilder builder)
      throws QueryProcessException, IOException {
    builder.writeBoolean(
        evaluator.evaluate(
            firstValues, firstIndex, secondValues, secondIndex, thirdValues, thirdIndex));
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }
}
