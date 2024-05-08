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

package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class ConfigTaskResult {
  private TSStatusCode statusCode;
  private TsBlock resultSet;
  private DatasetHeader resultSetHeader;

  public ConfigTaskResult(TSStatusCode statusCode) {
    this.statusCode = statusCode;
  }

  public ConfigTaskResult(
      TSStatusCode statusCode, TsBlock resultSet, DatasetHeader resultSetHeader) {
    this.statusCode = statusCode;
    this.resultSet = resultSet;
    this.resultSetHeader = resultSetHeader;
  }

  public TSStatusCode getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(TSStatusCode statusCode) {
    this.statusCode = statusCode;
  }

  public TsBlock getResultSet() {
    return resultSet;
  }

  public void setResultSet(TsBlock resultSet) {
    this.resultSet = resultSet;
  }

  public DatasetHeader getResultSetHeader() {
    return resultSetHeader;
  }
}
