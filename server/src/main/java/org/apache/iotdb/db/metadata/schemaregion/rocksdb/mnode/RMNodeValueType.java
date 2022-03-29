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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode;

import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants;

public enum RMNodeValueType {
  TTL(RSchemaConstants.FLAG_SET_TTL, RSchemaConstants.DATA_BLOCK_TYPE_TTL),
  SCHEMA(RSchemaConstants.FLAG_HAS_SCHEMA, RSchemaConstants.DATA_BLOCK_TYPE_SCHEMA),
  ALIAS(RSchemaConstants.FLAG_HAS_ALIAS, RSchemaConstants.DATA_BLOCK_TYPE_ALIAS),
  TAGS(RSchemaConstants.FLAG_HAS_TAGS, RSchemaConstants.DATA_BLOCK_TYPE_TAGS),
  ATTRIBUTES(RSchemaConstants.FLAG_HAS_ATTRIBUTES, RSchemaConstants.DATA_BLOCK_TYPE_ATTRIBUTES),
  ORIGIN_KEY(null, RSchemaConstants.DATA_BLOCK_TYPE_ORIGIN_KEY);

  private byte type;
  private Byte flag;

  RMNodeValueType(Byte flag, byte type) {
    this.type = type;
    this.flag = flag;
  }

  public byte getType() {
    return type;
  }

  public Byte getFlag() {
    return flag;
  }
}
