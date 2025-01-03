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

package org.apache.iotdb.confignode.consensus.response.table;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TDescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TTableColumnInfo;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.Map;

public class DescTable4InformationSchemaResp implements DataSet {
  private final TSStatus status;
  private final Map<String, Map<String, TTableColumnInfo>> tableColumnInfoMap;

  public DescTable4InformationSchemaResp(
      final TSStatus status, final Map<String, Map<String, TTableColumnInfo>> tableColumnInfoMap) {
    this.status = status;
    this.tableColumnInfoMap = tableColumnInfoMap;
  }

  public TDescTable4InformationSchemaResp convertToTDescTable4InformationSchemaResp() {
    return new TDescTable4InformationSchemaResp(status).setTableColumnInfoMap(tableColumnInfoMap);
  }
}
