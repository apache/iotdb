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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class WrappedStatement extends Statement {
  private static final Logger LOGGER = LoggerFactory.getLogger(WrappedStatement.class);

  protected org.apache.iotdb.db.queryengine.plan.statement.Statement innerTreeStatement;
  protected MPPQueryContext context;

  protected WrappedStatement(
      org.apache.iotdb.db.queryengine.plan.statement.Statement innerTreeStatement,
      MPPQueryContext context) {
    super(null);
    this.innerTreeStatement = innerTreeStatement;

    this.context = context;

    // 检查 measurements 是否有 null
    if (innerTreeStatement instanceof InsertBaseStatement) {
      InsertBaseStatement insertStatement = (InsertBaseStatement) innerTreeStatement;
      String[] measurements = insertStatement.getMeasurements();
      if (measurements != null) {
        List<Integer> nullIndices = new ArrayList<>();
        for (int i = 0; i < measurements.length; i++) {
          if (measurements[i] == null) {
            nullIndices.add(i);
          }
        }
        if (!nullIndices.isEmpty()) {
          String devicePath =
              insertStatement.getDevicePath() != null
                  ? insertStatement.getDevicePath().getFullPath()
                  : "null";
          LOGGER.error(
              "Found {} null measurements at indices {} in WrappedStatement constructor. DevicePath={}, Measurements={}, TotalCount={}, DataTypes={}, ColumnCategories={}",
              nullIndices.size(),
              nullIndices,
              devicePath,
              Arrays.toString(measurements),
              measurements.length,
              insertStatement.getDataTypes() != null
                  ? Arrays.toString(insertStatement.getDataTypes())
                  : "null",
              insertStatement.getColumnCategories() != null
                  ? Arrays.toString(insertStatement.getColumnCategories())
                  : "null");
        }
      }
    }
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return innerTreeStatement.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WrappedStatement that = (WrappedStatement) o;
    return Objects.equals(innerTreeStatement, that.innerTreeStatement);
  }

  @Override
  public String toString() {
    return innerTreeStatement.toString();
  }

  public org.apache.iotdb.db.queryengine.plan.statement.Statement getInnerTreeStatement() {
    return innerTreeStatement;
  }

  public void setInnerTreeStatement(
      org.apache.iotdb.db.queryengine.plan.statement.Statement innerTreeStatement) {
    this.innerTreeStatement = innerTreeStatement;
  }

  public MPPQueryContext getContext() {
    return context;
  }

  public void setContext(MPPQueryContext context) {
    this.context = context;
  }
}
