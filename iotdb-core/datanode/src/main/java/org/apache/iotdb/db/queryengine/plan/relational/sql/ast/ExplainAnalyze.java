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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ExplainAnalyze extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExplainAnalyze.class);

  private final Statement statement;
  private final boolean verbose;
  private final ExplainOutputFormat outputFormat;

  public ExplainAnalyze(Statement statement, boolean verbose) {
    super(null);
    this.statement =
        requireNonNull(statement, DataNodeQueryMessages.EXCEPTION_STATEMENT_IS_NULL_693A0622);
    this.verbose = verbose;
    this.outputFormat = ExplainOutputFormat.TEXT;
  }

  public ExplainAnalyze(Statement statement, boolean verbose, ExplainOutputFormat outputFormat) {
    super(null);
    this.statement = requireNonNull(statement, "statement is null");
    this.verbose = verbose;
    this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
  }

  public ExplainAnalyze(NodeLocation location, boolean verbose, Statement statement) {
    super(requireNonNull(location, DataNodeQueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.statement =
        requireNonNull(statement, DataNodeQueryMessages.EXCEPTION_STATEMENT_IS_NULL_693A0622);
    this.verbose = verbose;
    this.outputFormat = ExplainOutputFormat.TEXT;
  }

  public ExplainAnalyze(
      NodeLocation location,
      boolean verbose,
      Statement statement,
      ExplainOutputFormat outputFormat) {
    super(requireNonNull(location, "location is null"));
    this.statement = requireNonNull(statement, "statement is null");
    this.verbose = verbose;
    this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
  }

  public Statement getStatement() {
    return statement;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public ExplainOutputFormat getOutputFormat() {
    return outputFormat;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitExplainAnalyze(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(statement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, verbose, outputFormat);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ExplainAnalyze o = (ExplainAnalyze) obj;
    return Objects.equals(statement, o.statement)
        && verbose == o.verbose
        && outputFormat == o.outputFormat;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statement", statement)
        .add("verbose", verbose)
        .add("outputFormat", outputFormat)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(statement);
    return size;
  }
}
