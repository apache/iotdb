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

package org.apache.iotdb.db.query.udf.core.transformer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class ArithmeticBinaryTransformer extends Transformer {

  private final LayerPointReader leftPointReader;
  private final LayerPointReader rightPointReader;

  protected ArithmeticBinaryTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    this.leftPointReader = leftPointReader;
    this.rightPointReader = rightPointReader;
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    if (!leftPointReader.next() || !rightPointReader.next()) {
      return false;
    }
    if (!cacheTime()) {
      return false;
    }
    cachedDouble =
        evaluate(
            castCurrentValueToDoubleOperand(leftPointReader),
            castCurrentValueToDoubleOperand(rightPointReader));
    leftPointReader.readyForNext();
    rightPointReader.readyForNext();
    return true;
  }

  /**
   * finds the smallest, unconsumed timestamp that exists in both {@code leftPointReader} and {@code
   * rightPointReader} and then caches the timestamp in {@code cachedTime}.
   *
   * @return true if there has a timestamp that meets the requirements
   */
  private boolean cacheTime() throws IOException, QueryProcessException {
    long leftTime = leftPointReader.currentTime();
    long rightTime = rightPointReader.currentTime();

    while (leftTime != rightTime) {
      if (leftTime < rightTime) {
        leftPointReader.readyForNext();
        if (!leftPointReader.next()) {
          return false;
        }
        leftTime = leftPointReader.currentTime();
      } else {
        rightPointReader.readyForNext();
        if (!rightPointReader.next()) {
          return false;
        }
        rightTime = rightPointReader.currentTime();
      }
    }

    // leftTime == rightTime
    cachedTime = leftTime;
    return true;
  }

  protected abstract double evaluate(double leftOperand, double rightOperand);

  private static double castCurrentValueToDoubleOperand(LayerPointReader layerPointReader)
      throws IOException, QueryProcessException {
    switch (layerPointReader.getDataType()) {
      case INT32:
        return layerPointReader.currentInt();
      case INT64:
        return layerPointReader.currentLong();
      case FLOAT:
        return layerPointReader.currentFloat();
      case DOUBLE:
        return layerPointReader.currentDouble();
      default:
        throw new QueryProcessException(
            "Unsupported data type: " + layerPointReader.getDataType().toString());
    }
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.DOUBLE;
  }
}
