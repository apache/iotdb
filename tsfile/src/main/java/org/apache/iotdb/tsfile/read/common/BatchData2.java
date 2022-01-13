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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsVector;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Potentially optimized Version of {@link BatchData}.
 */
public class BatchData2 extends BatchData {

  private Supplier<Object> returner = null;

  public BatchData2(TSDataType type) {
    init(type);
  }

  @Override
  public void init(TSDataType type) {
    super.init(type);
    // Now also set an returner
    switch (type) {
      case INT32:
        this.returner = this::getInt;
        break;
      case FLOAT:
        this.returner = this::getFloat;
        break;
      default:
        throw new NotImplementedException();
    }
  }

  @Override
  public Object currentValue() {
    return this.returner.get();
  }
}
