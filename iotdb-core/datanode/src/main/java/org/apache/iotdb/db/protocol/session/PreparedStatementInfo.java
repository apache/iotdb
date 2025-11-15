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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Information about a prepared statement stored in a session. The AST is cached here to avoid
 * re-parsing on EXECUTE.
 */
public class PreparedStatementInfo {

  private final String statementName;
  private final Statement sql; // Cached AST (contains Parameter nodes)
  private final long createTime;
  private final IMemoryBlock memoryBlock; // Memory block allocated for this PreparedStatement

  public PreparedStatementInfo(String statementName, Statement sql, IMemoryBlock memoryBlock) {
    this.statementName = requireNonNull(statementName, "statementName is null");
    this.sql = requireNonNull(sql, "sql is null");
    this.createTime = System.currentTimeMillis();
    this.memoryBlock = memoryBlock;
  }

  public PreparedStatementInfo(
      String statementName, Statement sql, long createTime, IMemoryBlock memoryBlock) {
    this.statementName = requireNonNull(statementName, "statementName is null");
    this.sql = requireNonNull(sql, "sql is null");
    this.createTime = createTime;
    this.memoryBlock = memoryBlock;
  }

  public String getStatementName() {
    return statementName;
  }

  public Statement getSql() {
    return sql;
  }

  public long getCreateTime() {
    return createTime;
  }

  public IMemoryBlock getMemoryBlock() {
    return memoryBlock;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PreparedStatementInfo that = (PreparedStatementInfo) o;
    return Objects.equals(statementName, that.statementName) && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementName, sql);
  }

  @Override
  public String toString() {
    return "PreparedStatementInfo{"
        + "statementName='"
        + statementName
        + '\''
        + ", sql="
        + sql
        + ", createTime="
        + createTime
        + '}';
  }
}
