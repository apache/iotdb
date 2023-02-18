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

package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

public class RSchemaConstants {

  public static final char ZERO = '0';
  public static final char ROOT_CHAR = 'r';
  public static final String ROOT = "r";
  public static final String ROOT_STRING = "root";

  public static final String PATH_SEPARATOR = ".";
  public static final String ESCAPE_PATH_SEPARATOR = "[.]";

  public static final String TABLE_NAME_TAGS = "tags";

  // Node type
  public static final char NODE_TYPE_ROOT = '\u0000';
  public static final char NODE_TYPE_INTERNAL = '\u0001';
  public static final char NODE_TYPE_SG = '\u0002';
  public static final char NODE_TYPE_ENTITY = '\u0004';
  public static final char NODE_TYPE_MEASUREMENT = '\u0008';
  public static final char NODE_TYPE_ALIAS = '\u0010';

  public static final int MAX_NODE_TYPE_NUM = NODE_TYPE_ALIAS + 1;

  public static final Character[] ALL_NODE_TYPE_ARRAY =
      new Character[] {
        NODE_TYPE_ROOT,
        NODE_TYPE_INTERNAL,
        NODE_TYPE_SG,
        NODE_TYPE_ENTITY,
        NODE_TYPE_MEASUREMENT,
        NODE_TYPE_ALIAS
      };

  public static final byte DATA_VERSION = 0x00;

  public static final byte DEFAULT_FLAG = 0x00;

  public static final byte FLAG_SET_TTL = 0x01;
  public static final byte FLAG_HAS_SCHEMA = 0x01 << 1;
  public static final byte FLAG_HAS_ALIAS = 0x01 << 2;
  public static final byte FLAG_HAS_TAGS = 0x01 << 3;
  public static final byte FLAG_HAS_ATTRIBUTES = 0x01 << 4;
  public static final byte FLAG_IS_ALIGNED = 0x01 << 5;

  public static final byte DATA_BLOCK_TYPE_TTL = 0x01;
  public static final byte DATA_BLOCK_TYPE_SCHEMA = 0x01 << 1;
  public static final byte DATA_BLOCK_TYPE_ALIAS = 0x01 << 2;
  public static final byte DATA_BLOCK_TYPE_TAGS = 0x01 << 3;
  public static final byte DATA_BLOCK_TYPE_ATTRIBUTES = 0x01 << 4;
  // alias's origin key
  public static final byte DATA_BLOCK_TYPE_ORIGIN_KEY = 0x01 << 5;

  public static final byte[] DEFAULT_NODE_VALUE = new byte[] {DATA_VERSION, DEFAULT_FLAG};
  public static final byte[] DEFAULT_ALIGNED_ENTITY_VALUE =
      new byte[] {DATA_VERSION, FLAG_IS_ALIGNED};
}
