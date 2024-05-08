/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.udf.api.type;

/** A substitution class for TsDataType in UDF APIs. */
public enum Type {
  /** BOOLEAN */
  BOOLEAN((byte) 0),

  /** INT32 */
  INT32((byte) 1),

  /** INT64 */
  INT64((byte) 2),

  /** FLOAT */
  FLOAT((byte) 3),

  /** DOUBLE */
  DOUBLE((byte) 4),

  /** TEXT */
  TEXT((byte) 5);

  private final byte type;

  Type(byte type) {
    this.type = type;
  }

  public byte getType() {
    return type;
  }
}
