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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.CopyToOptions;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class CopyTo extends Statement {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(CopyTo.class);
  private final Statement queryStatement;
  private final String targetFileName;
  private final CopyToOptions options;

  public CopyTo(Statement queryStatement, String targetFileName, CopyToOptions options) {
    this(null, queryStatement, targetFileName, options);
  }

  public CopyTo(
      @Nullable NodeLocation location,
      Statement queryStatement,
      String targetFileName,
      CopyToOptions options) {
    super(location);
    this.queryStatement = queryStatement;
    this.targetFileName = targetFileName;
    this.options = options;
  }

  public Statement getQueryStatement() {
    return queryStatement;
  }

  public String getTargetFileName() {
    return targetFileName;
  }

  public CopyToOptions getOptions() {
    return options;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitCopyTo(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder().add(queryStatement).build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    CopyTo copyTo = (CopyTo) o;
    return Objects.equals(queryStatement, copyTo.queryStatement)
        && Objects.equals(targetFileName, copyTo.targetFileName)
        && Objects.equals(options, copyTo.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryStatement, targetFileName, options);
  }

  @Override
  public String toString() {
    return "CopyTo{"
        + "queryStatement="
        + queryStatement
        + ", targetFileName='"
        + targetFileName
        + '\''
        + ", options="
        + options
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(queryStatement);
    size += RamUsageEstimator.sizeOf(targetFileName);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(options);
    return size;
  }
}
