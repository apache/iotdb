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
package org.apache.iotdb.tsfile.file.metadata.enums;

public enum TSFreqType {

  SINGLE_FREQ, MULTI_FREQ, IRREGULAR_FREQ;

  /**
   * return deserialize type.
   *
   * @param i -param for deserialize
   * @return -ts frequency type
   */
  public static TSFreqType deserialize(short i) {
    switch (i) {
      case 0:
        return SINGLE_FREQ;
      case 1:
        return MULTI_FREQ;
      case 2:
        return IRREGULAR_FREQ;
      default:
        return IRREGULAR_FREQ;
    }
  }

  /**
   * return deserialize type.
   *
   * @return -ts frequency type
   */
  public short serialize() {
    switch (this) {
      case SINGLE_FREQ:
        return 0;
      case MULTI_FREQ:
        return 1;
      case IRREGULAR_FREQ:
        return 2;
      default:
        return 2;
    }
  }
}