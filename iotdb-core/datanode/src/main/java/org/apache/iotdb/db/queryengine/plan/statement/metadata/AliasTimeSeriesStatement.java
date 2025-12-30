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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * ALIAS TIMESERIES statement for renaming timeseries path (alias series).
 *
 * <p>This statement is used to rename a timeseries path, which will create an alias series pointing
 * to the original physical series. The syntax is:
 *
 * <p>ALTER TIMESERIES <oldPath> RENAME TO <newPath>
 */
public class AliasTimeSeriesStatement extends AlterTimeSeriesStatement implements IConfigStatement {

  /** Used when renaming the timeseries path itself (RENAME_TO). */
  private PartialPath newPath;

  private boolean isGeneratedByPipe = false;

  public AliasTimeSeriesStatement() {
    super();
    this.statementType = StatementType.ALTER_TIME_SERIES;
    // Set alterType to RENAME_TO for alias series renaming
    setAlterType(AlterType.RENAME_TO);
  }

  public AliasTimeSeriesStatement(MeasurementPath oldPath, PartialPath newPath) {
    this();
    setPath(oldPath);
    this.newPath = newPath;
  }

  public PartialPath getNewPath() {
    return newPath;
  }

  public void setNewPath(PartialPath newPath) {
    this.newPath = newPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    PartialPath oldPath = getPath();
    PartialPath newPath = getNewPath();
    if (oldPath != null && newPath != null) {
      return Arrays.asList(oldPath, newPath);
    } else if (oldPath != null) {
      return Arrays.asList(oldPath);
    } else {
      return super.getPaths();
    }
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAliasTimeSeriesStatement(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  public void markIsGeneratedByPipe() {
    isGeneratedByPipe = true;
  }

  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    final AliasTimeSeriesStatement that = (AliasTimeSeriesStatement) obj;
    return Objects.equals(this.newPath, that.newPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), newPath);
  }
}
