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
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.Objects;
import java.util.Set;

public class DescTableResp implements DataSet {
  private final TSStatus status;
  private final TsTable table;
  private final Set<String> preDeletedColumns;

  public DescTableResp(
      final TSStatus status, final TsTable table, final Set<String> preDeletedColumns) {
    this.status = status;
    this.table = table;
    this.preDeletedColumns = preDeletedColumns;
  }

  public TDescTableResp convertToTDescTableResp() {
    final TDescTableResp resp =
        new TDescTableResp(status)
            .setTableInfo(
                Objects.nonNull(table)
                    ? TsTableInternalRPCUtil.serializeSingleTsTable(table)
                    : null);
    return Objects.nonNull(preDeletedColumns) ? resp.setPreDeletedColumns(preDeletedColumns) : resp;
  }
}
