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
  PATH_CONTAINS((short) 2),
  DATA_TYPE((short) 3),
  VIEW_TYPE((short) 4),
  AND((short) 5),
  TEMPLATE_FILTER((short) 6),
  ID((short) 7),
  ATTRIBUTE((short) 8),

  OR((short) 9),
  NOT((short) 10),
  PRECISE((short) 11),
  IN((short) 12),
  LIKE((short) 13),
  COMPARISON((short) 14),
  ;

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
      case 3:
        return DATA_TYPE;
      case 4:
        return VIEW_TYPE;
      case 5:
        return AND;
      case 6:
        return TEMPLATE_FILTER;
      case 7:
        return ID;
      case 8:
        return ATTRIBUTE;
      case 9:
        return OR;
      case 10:
        return NOT;
      case 11:
        return PRECISE;
      case 12:
        return IN;
      case 13:
        return LIKE;
      case 14:
        return COMPARISON;
      default:
        throw new IllegalArgumentException("Invalid input: " + code);
    }
  }
}
