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

public class Explain extends Statement {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Explain.class);

  private final Statement statement;
  private final ExplainOutputFormat outputFormat;

  public Explain(Statement statement) {
    super(null);
    this.statement =
        requireNonNull(statement, DataNodeQueryMessages.EXCEPTION_STATEMENT_IS_NULL_693A0622);
    this.outputFormat = ExplainOutputFormat.GRAPHVIZ;
  }

  public Explain(Statement statement, ExplainOutputFormat outputFormat) {
    super(null);
    this.statement =
        requireNonNull(statement, DataNodeQueryMessages.EXCEPTION_STATEMENT_IS_NULL_693A0622);
    this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
  }

  public Explain(NodeLocation location, Statement statement) {
    super(requireNonNull(location, DataNodeQueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.statement =
        requireNonNull(statement, DataNodeQueryMessages.EXCEPTION_STATEMENT_IS_NULL_693A0622);
    this.outputFormat = ExplainOutputFormat.GRAPHVIZ;
  }

  public Explain(NodeLocation location, Statement statement, ExplainOutputFormat outputFormat) {
    super(requireNonNull(location, DataNodeQueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.statement =
        requireNonNull(statement, DataNodeQueryMessages.EXCEPTION_STATEMENT_IS_NULL_693A0622);
    this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
  }

  public Statement getStatement() {
    return statement;
  }

  public ExplainOutputFormat getOutputFormat() {
    return outputFormat;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitExplain(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder().add(statement).build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, outputFormat);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Explain o = (Explain) obj;
    return Objects.equals(statement, o.statement) && outputFormat == o.outputFormat;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statement", statement)
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
