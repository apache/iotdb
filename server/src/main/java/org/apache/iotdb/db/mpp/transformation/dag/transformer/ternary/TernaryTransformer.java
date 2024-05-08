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
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class TernaryTransformer extends Transformer {
  protected final LayerPointReader firstPointReader;
  protected final LayerPointReader secondPointReader;
  protected final LayerPointReader thirdPointReader;

  protected final TSDataType firstPointReaderDataType;
  protected final TSDataType secondPointReaderDataType;
  protected final TSDataType thirdPointReaderDataType;

  protected final boolean isFirstPointReaderConstant;
  protected final boolean isSecondPointReaderConstant;
  protected final boolean isThirdPointReaderConstant;

  protected final boolean isCurrentConstant;

  @Override
  protected YieldableState yieldValue() throws QueryProcessException, IOException {
    final YieldableState firstYieldableState = firstPointReader.yield();
    final YieldableState secondYieldableState = secondPointReader.yield();
    final YieldableState thirdYieldableState = thirdPointReader.yield();

    if (YieldableState.NOT_YIELDABLE_NO_MORE_DATA.equals(firstYieldableState)
        || YieldableState.NOT_YIELDABLE_NO_MORE_DATA.equals(secondYieldableState)
        || YieldableState.NOT_YIELDABLE_NO_MORE_DATA.equals(thirdYieldableState)) {
      return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
    }

    if (YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA.equals(firstYieldableState)
        || YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA.equals(secondYieldableState)
        || YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA.equals(thirdYieldableState)) {
      return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
    }

    final YieldableState timeYieldState = yieldTime();
    if (!YieldableState.YIELDABLE.equals(timeYieldState)) {
      return timeYieldState;
    }

    if (firstPointReader.isCurrentNull()
        || secondPointReader.isCurrentNull()
        || thirdPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      transformAndCache();
    }

    firstPointReader.readyForNext();
    secondPointReader.readyForNext();
    thirdPointReader.readyForNext();
    return YieldableState.YIELDABLE;
  }

  private YieldableState yieldTime() throws IOException, QueryProcessException {
    if (isCurrentConstant) {
      return YieldableState.YIELDABLE;
    }

    long firstTime = isFirstPointReaderConstant ? Long.MIN_VALUE : firstPointReader.currentTime();
    long secondTime =
        isSecondPointReaderConstant ? Long.MIN_VALUE : secondPointReader.currentTime();
    long thirdTime = isThirdPointReaderConstant ? Long.MIN_VALUE : thirdPointReader.currentTime();

    while (firstTime != secondTime || firstTime != thirdTime) { // the logic is similar to MergeSort
      if (firstTime < secondTime) {
        if (isFirstPointReaderConstant) {
          firstTime = secondTime;
        } else {
          firstPointReader.readyForNext();
          final YieldableState firstYieldableState = firstPointReader.yield();
          if (!YieldableState.YIELDABLE.equals(firstYieldableState)) {
            return firstYieldableState;
          }
          firstTime = firstPointReader.currentTime();
        }
      } else if (secondTime < thirdTime) {
        if (isSecondPointReaderConstant) {
          secondTime = thirdTime;
        } else {
          secondPointReader.readyForNext();
          final YieldableState secondYieldableState = secondPointReader.yield();
          if (!YieldableState.YIELDABLE.equals(secondYieldableState)) {
            return secondYieldableState;
          }
          secondTime = secondPointReader.currentTime();
        }
      } else {
        if (isThirdPointReaderConstant) {
          thirdTime = firstTime;
        } else {
          thirdPointReader.readyForNext();
          final YieldableState thirdYieldableState = thirdPointReader.yield();
          if (!YieldableState.YIELDABLE.equals(thirdYieldableState)) {
            return thirdYieldableState;
          }
          thirdTime = thirdPointReader.currentTime();
        }
      }
    }

    if (firstTime != Long.MIN_VALUE) {
      cachedTime = firstTime;
    }
    return YieldableState.YIELDABLE;
  }

  protected TernaryTransformer(
      LayerPointReader firstPointReader,
      LayerPointReader secondPointReader,
      LayerPointReader thirdPointReader) {
    this.firstPointReader = firstPointReader;
    this.secondPointReader = secondPointReader;
    this.thirdPointReader = thirdPointReader;
    this.firstPointReaderDataType = firstPointReader.getDataType();
    this.secondPointReaderDataType = secondPointReader.getDataType();
    this.thirdPointReaderDataType = thirdPointReader.getDataType();
    this.isFirstPointReaderConstant = firstPointReader.isConstantPointReader();
    this.isSecondPointReaderConstant = secondPointReader.isConstantPointReader();
    this.isThirdPointReaderConstant = thirdPointReader.isConstantPointReader();
    this.isCurrentConstant =
        isFirstPointReaderConstant && isSecondPointReaderConstant && isThirdPointReaderConstant;
    checkType();
  }

  @Override
  public boolean isConstantPointReader() {
    return firstPointReader.isConstantPointReader()
        && secondPointReader.isConstantPointReader()
        && thirdPointReader.isConstantPointReader();
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    if (!firstPointReader.next() || !secondPointReader.next() || !thirdPointReader.next()) {
      return false;
    }

    if (!cacheTime()) {
      return false;
    }

    if (firstPointReader.isCurrentNull()
        || secondPointReader.isCurrentNull()
        || thirdPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      transformAndCache();
    }

    firstPointReader.readyForNext();
    secondPointReader.readyForNext();
    thirdPointReader.readyForNext();
    return true;
  }

  protected abstract void transformAndCache() throws QueryProcessException, IOException;

  protected abstract void checkType();

  /**
   * finds the smallest, unconsumed, same timestamp that exists in {@code firstPointReader}, {@code
   * secondPointReader} and {@code thirdPointReader}and then caches the timestamp in {@code
   * cachedTime}.
   *
   * @return true if there has a timestamp that meets the requirements
   */
  private boolean cacheTime() throws IOException, QueryProcessException {
    boolean isFirstConstant = firstPointReader.isConstantPointReader();
    boolean isSecondConstant = secondPointReader.isConstantPointReader();
    boolean isThirdConstant = thirdPointReader.isConstantPointReader();
    long firstTime = isFirstConstant ? Long.MIN_VALUE : firstPointReader.currentTime();
    long secondTime = isSecondConstant ? Long.MIN_VALUE : secondPointReader.currentTime();
    long thirdTime = isThirdConstant ? Long.MIN_VALUE : secondPointReader.currentTime();
    // Long.MIN_VALUE is used to determine whether  isFirstConstant && isSecondConstant &&
    // isThirdConstant = true
    while (firstTime != secondTime || firstTime != thirdTime) { // the logic is similar to MergeSort
      if (firstTime < secondTime) {
        if (isFirstConstant) {
          firstTime = secondTime;
        } else {
          firstPointReader.readyForNext();
          if (!firstPointReader.next()) {
            return false;
          }
          firstTime = firstPointReader.currentTime();
        }
      } else if (secondTime < thirdTime) {
        if (isSecondConstant) {
          secondTime = thirdTime;
        } else {
          secondPointReader.readyForNext();
          if (!secondPointReader.next()) {
            return false;
          }
          secondTime = secondPointReader.currentTime();
        }
      } else {
        if (isThirdConstant) {
          thirdTime = firstTime;
        } else {
          thirdPointReader.readyForNext();
          if (!thirdPointReader.next()) {
            return false;
          }
          thirdTime = secondPointReader.currentTime();
        }
      }
    }

    if (firstTime != Long.MIN_VALUE) {
      cachedTime = firstTime;
    }
    return true;
  }

  protected static double castCurrentValueToDoubleOperand(
      LayerPointReader layerPointReader, TSDataType layerPointReaderDataType)
      throws IOException, QueryProcessException {
    switch (layerPointReaderDataType) {
      case INT32:
        return layerPointReader.currentInt();
      case INT64:
        return layerPointReader.currentLong();
      case FLOAT:
        return layerPointReader.currentFloat();
      case DOUBLE:
        return layerPointReader.currentDouble();
      case BOOLEAN:
        return layerPointReader.currentBoolean() ? 1.0d : 0.0d;
      default:
        throw new QueryProcessException(
            "Unsupported data type: " + layerPointReader.getDataType().toString());
    }
  }
}
