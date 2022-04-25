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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeleteStorageGroupReq extends ConfigRequest {

  private List<TStorageGroupSchema> deleteSgSchemaList;

  public List<TStorageGroupSchema> getDeleteSgSchemaList() {
    return deleteSgSchemaList;
  }

  public void setDeleteSgSchemaList(List<TStorageGroupSchema> deleteSgSchemaList) {
    this.deleteSgSchemaList = deleteSgSchemaList;
  }

  public DeleteStorageGroupReq() {
    super(ConfigRequestType.DeleteStorageGroup);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.DeleteStorageGroup.ordinal());
    if (deleteSgSchemaList != null && !deleteSgSchemaList.isEmpty()) {
      buffer.putInt(deleteSgSchemaList.size());
      for (TStorageGroupSchema schema : deleteSgSchemaList) {
        ThriftConfigNodeSerDeUtils.writeTStorageGroupSchema(schema, buffer);
      }
    } else {
      buffer.putInt(0);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    int size = buffer.getInt();
    if (size > 0) {
      deleteSgSchemaList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        deleteSgSchemaList.add(ThriftConfigNodeSerDeUtils.readTStorageGroupSchema(buffer));
      }
    }
  }
}
