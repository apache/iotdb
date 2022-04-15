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
package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageGroupSchemaDataSet implements DataSet {

  private TSStatus status;

  private List<StorageGroupSchema> schemaList;

  public StorageGroupSchemaDataSet() {}

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public List<StorageGroupSchema> getSchemaList() {
    return schemaList;
  }

  public void setSchemaList(List<StorageGroupSchema> schemaList) {
    this.schemaList = schemaList;
  }

  public void convertToRPCStorageGroupSchemaResp(TStorageGroupSchemaResp resp) {
    resp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      Map<String, TStorageGroupSchema> storageGroupMessageMap = new HashMap<>();
      for (StorageGroupSchema schema : schemaList) {
        storageGroupMessageMap.put(schema.getName(), new TStorageGroupSchema(schema.getName()));
      }
      resp.setStorageGroupSchemaMap(storageGroupMessageMap);
    }
  }
}
