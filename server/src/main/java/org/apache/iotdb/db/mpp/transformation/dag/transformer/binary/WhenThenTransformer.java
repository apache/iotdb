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

public class WhenThenTransformer extends BinaryTransformer {
  protected WhenThenTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  public TSDataType getDataType() {
    return rightPointReaderDataType;
  }

  @Override
  protected void checkType() {
    if (leftPointReaderDataType != TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException("WHEN expression must return BOOLEAN");
    }
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    switch (rightPointReaderDataType) {
      case BOOLEAN:
        cachedBoolean = rightPointReader.currentBoolean();
        break;
      case TEXT:
        cachedBinary = rightPointReader.currentBinary();
        break;
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        cachedDouble = castCurrentValueToDoubleOperand(rightPointReader, rightPointReaderDataType);
      default:
        throw new UnSupportedDataTypeException(rightPointReaderDataType.name());
    }
  }
}
