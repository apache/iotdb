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
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class UDFQueryTransformer extends Transformer {

  protected final UDTFExecutor executor;

  protected final TSDataType udfOutputDataType;
  protected final LayerPointReader udfOutput;

  protected boolean terminated;

  protected UDFQueryTransformer(UDTFExecutor executor) {
    this.executor = executor;
    udfOutputDataType = executor.getConfigurations().getOutputDataType();
    udfOutput = executor.getCollector().constructPointReaderUsingTrivialEvictionStrategy();
    terminated = false;
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    while (!cacheValueFromUDFOutput()) {
      if (!executeUDFOnce() && !terminate()) {
        return false;
      }
    }
    return true;
  }

  protected final boolean cacheValueFromUDFOutput() throws QueryProcessException, IOException {
    boolean hasNext = udfOutput.next();
    if (hasNext) {
      cachedTime = udfOutput.currentTime();
      switch (udfOutputDataType) {
        case INT32:
          cachedInt = udfOutput.currentInt();
          break;
        case INT64:
          cachedLong = udfOutput.currentLong();
          break;
        case FLOAT:
          cachedFloat = udfOutput.currentFloat();
          break;
        case DOUBLE:
          cachedDouble = udfOutput.currentDouble();
          break;
        case BOOLEAN:
          cachedBoolean = udfOutput.currentBoolean();
          break;
        case TEXT:
          cachedBinary = udfOutput.currentBinary();
          break;
        default:
          throw new UnSupportedDataTypeException(udfOutputDataType.toString());
      }
      udfOutput.readyForNext();
    }
    return hasNext;
  }

  protected abstract boolean executeUDFOnce() throws QueryProcessException, IOException;

  protected final boolean terminate() throws QueryProcessException {
    if (terminated) {
      return false;
    }
    executor.terminate();
    terminated = true;
    return true;
  }

  @Override
  public final TSDataType getDataType() {
    return udfOutputDataType;
  }
}
