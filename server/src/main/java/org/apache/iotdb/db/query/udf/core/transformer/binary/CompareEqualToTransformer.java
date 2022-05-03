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

package org.apache.iotdb.db.query.udf.core.transformer.binary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class CompareEqualToTransformer extends CompareBinaryTransformer {

  public CompareEqualToTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    if (leftPointReader.getDataType() == TSDataType.BOOLEAN
        && rightPointReader.getDataType() == TSDataType.BOOLEAN) {
      cachedBoolean = evaluate(leftPointReader.currentBoolean(), rightPointReader.currentBoolean());
    } else {
      cachedBoolean =
          evaluate(
              castCurrentValueToDoubleOperand(leftPointReader),
              castCurrentValueToDoubleOperand(rightPointReader));
    }
  }

  @Override
  protected boolean evaluate(double leftOperand, double rightOperand) {
    return Double.compare(leftOperand, rightOperand) == 0;
  }

  protected boolean evaluate(boolean leftOperand, boolean rightOperand) {
    return leftOperand == rightOperand;
  }
}
