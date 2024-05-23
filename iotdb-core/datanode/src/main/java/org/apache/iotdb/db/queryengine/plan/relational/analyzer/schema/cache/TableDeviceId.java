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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache;

import java.util.Arrays;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache.CacheMemoryControlUtil.estimateStringSize;

public class TableDeviceId {

  private final String[] idValues;

  public TableDeviceId(String[] idValues) {
    this.idValues = idValues;
  }

  public String getIdValue(int index) {
    return idValues[index];
  }

  public String[] getIdValues() {
    return idValues;
  }

  public int estimateSize() {
    int size = 8 + 8 + 8 + 4; // object header + reference + String[] header + String.length
    for (String node : idValues) {
      size += estimateStringSize(node);
    }
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableDeviceId)) return false;
    TableDeviceId that = (TableDeviceId) o;
    return Arrays.equals(idValues, that.idValues);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(idValues);
  }
}
