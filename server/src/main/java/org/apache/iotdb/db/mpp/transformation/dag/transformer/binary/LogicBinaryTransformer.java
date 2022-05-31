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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.binary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class LogicBinaryTransformer extends BinaryTransformer {

  protected LogicBinaryTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  protected void checkType() {
    if (leftPointReaderDataType != TSDataType.BOOLEAN
        || rightPointReaderDataType != TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException("Unsupported data type: " + TSDataType.BOOLEAN);
    }
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    final boolean leftHasNext = leftPointReader.next();
    final boolean rightHasNext = rightPointReader.next();

    if (leftHasNext && rightHasNext) {
      return cacheValue(leftPointReader, rightPointReader);
    }

    if (!leftHasNext && !rightHasNext) {
      return false;
    }

    if (leftHasNext && !isLeftPointReaderConstant) {
      return cacheValue(leftPointReader);
    }
    if (rightHasNext && !isRightPointReaderConstant) {
      return cacheValue(rightPointReader);
    }

    return false;
  }

  private boolean cacheValue(LayerPointReader reader) throws IOException {
    cachedTime = reader.currentTime();
    cachedBoolean = evaluate(false, leftPointReader.currentBoolean());
    leftPointReader.readyForNext();
    return true;
  }

  private boolean cacheValue(LayerPointReader leftPointReader, LayerPointReader rightPointReader)
      throws IOException {
    if (isCurrentConstant) {
      cachedBoolean = evaluate(leftPointReader.currentBoolean(), rightPointReader.currentBoolean());
      return true;
    }

    if (isLeftPointReaderConstant) {
      cachedTime = rightPointReader.currentTime();
      cachedBoolean = evaluate(leftPointReader.currentBoolean(), rightPointReader.currentBoolean());
      rightPointReader.readyForNext();
      return true;
    }

    if (isRightPointReaderConstant) {
      cachedTime = leftPointReader.currentTime();
      cachedBoolean = evaluate(leftPointReader.currentBoolean(), rightPointReader.currentBoolean());
      leftPointReader.readyForNext();
      return true;
    }

    final long leftTime = leftPointReader.currentTime();
    final long rightTime = rightPointReader.currentTime();

    if (leftTime < rightTime) {
      cachedTime = leftTime;
      cachedBoolean = evaluate(leftPointReader.currentBoolean(), false);
      leftPointReader.readyForNext();
      return true;
    }

    if (rightTime < leftTime) {
      cachedTime = rightTime;
      cachedBoolean = evaluate(false, rightPointReader.currentBoolean());
      rightPointReader.readyForNext();
      return true;
    }

    // == rightTime
    cachedTime = leftTime;
    cachedBoolean = evaluate(leftPointReader.currentBoolean(), rightPointReader.currentBoolean());
    leftPointReader.readyForNext();
    rightPointReader.readyForNext();
    return true;
  }

  protected abstract boolean evaluate(boolean leftOperand, boolean rightOperand);

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }
}
