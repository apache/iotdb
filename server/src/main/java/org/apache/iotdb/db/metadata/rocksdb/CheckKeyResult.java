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

package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.metadata.rocksdb.mnode.RMNodeType;

import static org.apache.iotdb.db.metadata.rocksdb.RSchemaUtils.NODE_TYPE_ARRAY;

public class CheckKeyResult {

  private byte[] value;
  private RMNodeType nodeType;

  public boolean existAnyKey() {
    return nodeType != null;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public void setExistType(char type) {
    nodeType = NODE_TYPE_ARRAY[type];
  }

  public RMNodeType getExistType() {
    return nodeType;
  }

  public boolean getResult(RMNodeType type) {
    if (type == nodeType) {
      return true;
    }
    return false;
  }
}
