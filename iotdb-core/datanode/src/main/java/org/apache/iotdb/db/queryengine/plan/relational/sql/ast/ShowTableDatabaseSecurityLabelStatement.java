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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableDatabaseSecurityLabelResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Statement for showing database security label in table model SHOW DATABASES (database)?
 * SECURITY_LABEL
 */
public class ShowTableDatabaseSecurityLabelStatement extends Statement {

  private final String database;

  public ShowTableDatabaseSecurityLabelStatement(@Nullable NodeLocation location, String database) {
    super(location);
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  public StatementType getStatementType() {
    return StatementType.SHOW_TABLE_DATABASE_SECURITY_LABEL;
  }

  /**
   * Build TsBlock for showing database security labels
   *
   * @param resp the response from ConfigNode containing security label data
   * @param future the future to set the result
   */
  public void buildTSBlockFromSecurityLabel(
      TShowTableDatabaseSecurityLabelResp resp, SettableFuture<ConfigTaskResult> future) {

    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    try {
      Map<String, String> securityLabelMap = resp.getSecurityLabel();

      if (securityLabelMap != null && !securityLabelMap.isEmpty()) {
        for (Map.Entry<String, String> entry : securityLabelMap.entrySet()) {
          String dbName = entry.getKey();
          String securityLabel = entry.getValue();

          // If database is specified, only show that database
          if (database != null && !database.equals(dbName)) {
            continue;
          }

          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder
              .getColumnBuilder(0)
              .writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));
          tsBlockBuilder
              .getColumnBuilder(1)
              .writeBinary(new Binary(securityLabel, TSFileConfig.STRING_CHARSET));
          tsBlockBuilder.declarePosition();
        }
      }

      future.set(
          new ConfigTaskResult(
              TSStatusCode.SUCCESS_STATUS,
              tsBlockBuilder.build(),
              DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader()));

    } catch (Exception e) {
      future.setException(e);
    }
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(database);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ShowTableDatabaseSecurityLabelStatement that = (ShowTableDatabaseSecurityLabelStatement) obj;
    return Objects.equals(database, that.database);
  }

  @Override
  public String toString() {
    return String.format("ShowTableDatabaseSecurityLabelStatement{database='%s'}", database);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowDatabaseSecurityLabel(this, context);
  }
}
