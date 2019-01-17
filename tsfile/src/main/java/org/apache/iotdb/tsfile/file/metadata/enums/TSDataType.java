/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.file.metadata.enums;

public enum TSDataType {
  BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT;

  /**
   * give an integer to return a data type.
   *
   * @param i -param to judge enum type
   * @return -enum type
   */
  public static TSDataType deserialize(short i) {
    assert i < 6;
    switch (i) {
      case 0:
        return BOOLEAN;
      case 1:
        return INT32;
      case 2:
        return INT64;
      case 3:
        return FLOAT;
      case 4:
        return DOUBLE;
      case 5:
        return TEXT;
      default:
        return TEXT;
    }
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  /**
   * return a serialize data type.
   *
   * @return -enum type
   */
  public short serialize() {
    switch (this) {
      case BOOLEAN:
        return 0;
      case INT32:
        return 1;
      case INT64:
        return 2;
      case FLOAT:
        return 3;
      case DOUBLE:
        return 4;
      case TEXT:
        return 5;
      default:
        return -1;
    }
  }
}
