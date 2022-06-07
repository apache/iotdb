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
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.input.ConstantInputReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.Objects;

public abstract class CompareBinaryTransformer extends BinaryTransformer {

  @FunctionalInterface
  protected interface Evaluator {

    boolean evaluate() throws QueryProcessException, IOException;
  }

  protected final Evaluator evaluator;

  protected CompareBinaryTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader)
      throws UnSupportedDataTypeException {
    super(leftPointReader, rightPointReader);
    transformFloatConstant();
    evaluator =
        TSDataType.TEXT.equals(leftPointReaderDataType)
            ? constructTextEvaluator()
            : constructNumberEvaluator();
  }

  protected abstract Evaluator constructNumberEvaluator();

  protected abstract Evaluator constructTextEvaluator();

  /**
   * When LayerPointReader returns float, constant layerPointReader should be cast to float.
   * Otherwise, precision error could occur. For example, when comparing s1 >= 3.33, if s1 is float
   * and its value is 3.33, s1 >= 3.33 would return false because 3.33f would be
   * 3.3299999237060547d.
   */
  protected void transformFloatConstant() {
    if (isLeftPointReaderConstant) {
      if (!isRightPointReaderConstant) {
        if (rightPointReaderDataType.equals(TSDataType.FLOAT)) {
          try {
            ConstantOperand floatConstant =
                new ConstantOperand(
                    TSDataType.FLOAT,
                    ((ConstantInputReader) leftPointReader).getConstantOperand().getValueString());
            leftPointReader = new ConstantInputReader(floatConstant);
            leftPointReaderDataType = TSDataType.FLOAT;
          } catch (QueryProcessException ignored) {
            // expect no exception since LayerPointReader has been initialized once
          }
        }
      }
    }
    if (isRightPointReaderConstant) {
      if (!isLeftPointReaderConstant) {
        if (leftPointReaderDataType.equals(TSDataType.FLOAT)) {
          try {
            ConstantOperand floatConstant =
                new ConstantOperand(
                    TSDataType.FLOAT,
                    ((ConstantInputReader) rightPointReader).getConstantOperand().getValueString());
            rightPointReader = new ConstantInputReader(floatConstant);
            rightPointReaderDataType = TSDataType.FLOAT;
          } catch (QueryProcessException ignored) {
            // expect no exception since LayerPointReader has been initialized once
          }
        }
      }
    }
  }

  protected static int compare(CharSequence cs1, CharSequence cs2) {
    if (Objects.requireNonNull(cs1) == Objects.requireNonNull(cs2)) {
      return 0;
    }

    if (cs1.getClass() == cs2.getClass() && cs1 instanceof Comparable) {
      return ((Comparable<Object>) cs1).compareTo(cs2);
    }

    for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
      char a = cs1.charAt(i);
      char b = cs2.charAt(i);
      if (a != b) {
        return a - b;
      }
    }

    return cs1.length() - cs2.length();
  }

  @Override
  protected final void checkType() {
    if (leftPointReaderDataType.equals(rightPointReaderDataType)) {
      return;
    }

    if (leftPointReaderDataType.equals(TSDataType.BOOLEAN)
        || rightPointReaderDataType.equals(TSDataType.BOOLEAN)) {
      throw new UnSupportedDataTypeException(TSDataType.BOOLEAN.toString());
    }
    if (leftPointReaderDataType.equals(TSDataType.TEXT)
        || rightPointReaderDataType.equals(TSDataType.TEXT)) {
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
