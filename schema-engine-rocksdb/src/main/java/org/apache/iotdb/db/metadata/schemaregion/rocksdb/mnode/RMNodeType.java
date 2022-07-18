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

import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_ALIAS;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_ENTITY;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_INTERNAL;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_MEASUREMENT;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_SG;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaUtils.NODE_TYPE_ARRAY;

public enum RMNodeType {
  INTERNAL(NODE_TYPE_INTERNAL),
  STORAGE_GROUP(NODE_TYPE_SG),
  ENTITY(NODE_TYPE_ENTITY),
  MEASUREMENT(NODE_TYPE_MEASUREMENT),
  ALISA(NODE_TYPE_ALIAS);

  private char value;

  RMNodeType(char value) {
    this.value = value;
  }

  public RMNodeType of(char type) {
    return NODE_TYPE_ARRAY[type];
  }

  public char getValue() {
    return value;
  }
}
