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

import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.MAX_NODE_TYPE_NUM;

public class CheckKeyResult {

  private boolean[] result = new boolean[MAX_NODE_TYPE_NUM];
  private boolean existAnyKey = false;
  private byte[] value;

  public void setSingleCheckValue(char index, boolean value) {
    if (value) {
      existAnyKey = true;
    }
    result[index] = value;
  }

  public boolean existAnyKey() {
    return existAnyKey;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public boolean getResult(RocksDBMNodeType type) {
    return result[type.value];
  }
}
