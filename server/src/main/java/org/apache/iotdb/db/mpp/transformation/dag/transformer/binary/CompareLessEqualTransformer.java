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

import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.util.TransformUtils;

public class CompareLessEqualTransformer extends CompareBinaryTransformer {

  public CompareLessEqualTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  protected Evaluator constructNumberEvaluator() {
    return () ->
        Double.compare(
                castCurrentValueToDoubleOperand(leftPointReader, leftPointReaderDataType),
                castCurrentValueToDoubleOperand(rightPointReader, rightPointReaderDataType))
            <= 0;
  }

  @Override
  protected Evaluator constructTextEvaluator() {
    return () ->
        TransformUtils.compare(
                leftPointReader.currentBinary().getStringValue(),
                rightPointReader.currentBinary().getStringValue())
            <= 0;
  }
}
