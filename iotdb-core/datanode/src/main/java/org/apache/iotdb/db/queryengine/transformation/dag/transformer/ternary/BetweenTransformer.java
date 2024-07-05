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

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TransformUtils;

import org.apache.tsfile.block.column.Column;

import static org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils.castValueToDouble;

public class BetweenTransformer extends CompareTernaryTransformer {

  boolean isNotBetween;

  public BetweenTransformer(
      LayerReader firstReader,
      LayerReader secondReader,
      LayerReader thirdReader,
      boolean isNotBetween) {
    super(firstReader, secondReader, thirdReader);
    this.isNotBetween = isNotBetween;
  }

  @Override
  protected Evaluator constructNumberEvaluator() {
    return (Column firstValues,
        int firstIndex,
        Column secondValues,
        int secondIndex,
        Column thirdValues,
        int thirdIndex) ->
        ((Double.compare(
                        castValueToDouble(firstValues, firstReaderDataType, firstIndex),
                        castValueToDouble(secondValues, secondReaderDataType, secondIndex))
                    >= 0)
                && (Double.compare(
                        castValueToDouble(firstValues, firstReaderDataType, firstIndex),
                        castValueToDouble(thirdValues, thirdReaderDataType, thirdIndex))
                    <= 0))
            ^ isNotBetween;
  }

  @Override
  protected Evaluator constructTextEvaluator() {
    return (Column firstValues,
        int firstIndex,
        Column secondValues,
        int secondIndex,
        Column thirdValues,
        int thirdIndex) ->
        ((TransformUtils.compare(
                        firstValues.getBinary(firstIndex), secondValues.getBinary(secondIndex))
                    >= 0)
                && (TransformUtils.compare(
                        firstValues.getBinary(firstIndex), thirdValues.getBinary(thirdIndex))
                    <= 0))
            ^ isNotBetween;
  }
}
