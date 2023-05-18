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
package org.apache.iotdb.commons.schema.filter;

public enum SchemaFilterType {
  NULL((short) -1),
  TAGS_FILTER((short) 1),
  PATH_CONTAINS((short) 2);

  private final short code;

  SchemaFilterType(short code) {
    this.code = code;
  }

  public short getCode() {
    return code;
  }

  public static SchemaFilterType getSchemaFilterType(short code) {
    switch (code) {
      case -1:
        return NULL;
      case 1:
        return TAGS_FILTER;
      case 2:
        return PATH_CONTAINS;
      default:
        throw new IllegalArgumentException("Invalid input: " + code);
    }
  }
}
