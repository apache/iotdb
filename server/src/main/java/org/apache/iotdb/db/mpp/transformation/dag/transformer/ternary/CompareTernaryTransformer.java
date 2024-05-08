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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class CompareTernaryTransformer extends TernaryTransformer {

  @FunctionalInterface
  public interface Evaluator {

    boolean evaluate() throws QueryProcessException, IOException;
  }

  protected final Evaluator evaluator;

  protected CompareTernaryTransformer(
      LayerPointReader firstPointReader,
      LayerPointReader secondPointReader,
      LayerPointReader thirdPointReader)
      throws UnSupportedDataTypeException {
    super(firstPointReader, secondPointReader, thirdPointReader);
    evaluator =
        TSDataType.TEXT.equals(firstPointReaderDataType)
            ? constructTextEvaluator()
            : constructNumberEvaluator();
  }

  protected abstract Evaluator constructNumberEvaluator();

  protected abstract Evaluator constructTextEvaluator();

  @Override
  protected final void checkType() {
    if ((firstPointReaderDataType).equals(secondPointReaderDataType)
        && (firstPointReaderDataType).equals(thirdPointReaderDataType)) {
      return;
    }

    if (firstPointReaderDataType.equals(TSDataType.BOOLEAN)
        || secondPointReaderDataType.equals(TSDataType.BOOLEAN)
        || thirdPointReaderDataType.equals(TSDataType.BOOLEAN)) {
      throw new UnSupportedDataTypeException(TSDataType.BOOLEAN.toString());
    }

    if (firstPointReaderDataType.equals(TSDataType.TEXT)
        || secondPointReaderDataType.equals(TSDataType.TEXT)
        || thirdPointReaderDataType.equals(TSDataType.TEXT)) {
      throw new UnSupportedDataTypeException(TSDataType.TEXT.toString());
    }
  }

  @Override
  protected final void transformAndCache() throws QueryProcessException, IOException {
    cachedBoolean = evaluator.evaluate();
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }
}
