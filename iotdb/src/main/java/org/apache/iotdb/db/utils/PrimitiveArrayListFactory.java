/**
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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class PrimitiveArrayListFactory {

  public static PrimitiveArrayList getByDataType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return new PrimitiveArrayList(boolean.class);
      case INT32:
        return new PrimitiveArrayList(int.class);
      case INT64:
        return new PrimitiveArrayList(long.class);
      case FLOAT:
        return new PrimitiveArrayList(float.class);
      case DOUBLE:
        return new PrimitiveArrayList(double.class);
      case TEXT:
        return new PrimitiveArrayList(Binary.class);
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
  }
}
