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
package org.apache.iotdb.confignode.consensus.request.read;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class CountStorageGroupReq extends ConfigRequest {

  private String[] storageGroupPattern;

  public CountStorageGroupReq() {
    super(ConfigRequestType.CountStorageGroup);
  }

  public CountStorageGroupReq(ConfigRequestType type) {
    super(type);
  }

  public CountStorageGroupReq(List<String> storageGroupPattern) {
    this();
    this.storageGroupPattern = storageGroupPattern.toArray(new String[0]);
  }

  public CountStorageGroupReq(ConfigRequestType type, List<String> storageGroupPattern) {
    super(type);
    this.storageGroupPattern = storageGroupPattern.toArray(new String[0]);
  }

  public String[] getStorageGroupPattern() {
    return storageGroupPattern;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getType().ordinal());

    buffer.putInt(storageGroupPattern.length);
    for (String node : storageGroupPattern) {
      BasicStructureSerDeUtil.write(node, buffer);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    int length = buffer.getInt();
    storageGroupPattern = new String[length];
    for (int i = 0; i < length; i++) {
      storageGroupPattern[i] = BasicStructureSerDeUtil.readString(buffer);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CountStorageGroupReq that = (CountStorageGroupReq) o;
    return Arrays.equals(storageGroupPattern, that.storageGroupPattern);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(storageGroupPattern);
  }
}
