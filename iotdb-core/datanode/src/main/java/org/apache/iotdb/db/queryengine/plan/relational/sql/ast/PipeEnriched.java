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

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Objects;

public class PipeEnriched extends Statement {

  private final Statement innerStatement;

  private final String originClusterId;

  public PipeEnriched(final @NotNull Statement innerstatement,final  String originCluster) {
    super(innerstatement.getLocation().isPresent() ? innerstatement.getLocation().get() : null);
    this.innerStatement = innerstatement;
    this.originClusterId = originCluster;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPipeEnriched(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return innerStatement.getChildren();
  }

  @Override
  public int hashCode() {
    return innerStatement.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeEnriched that = (PipeEnriched) obj;
    return Objects.equals(innerStatement, that.innerStatement);
  }

  @Override
  public String toString() {
    return innerStatement.toString();
  }

  public Statement getInnerStatement() {
    return innerStatement;
  }

  public String getOriginClusterId() {
    return originClusterId;
  }

}
