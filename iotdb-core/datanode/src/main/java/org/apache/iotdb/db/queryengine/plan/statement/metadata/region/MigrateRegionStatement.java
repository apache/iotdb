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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.region;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

/**
 * MIGRATE REGION statement
 *
 * <p>Here is the syntax definition:
 *
 * <p>MIGRATE REGION regionid=INTEGER_LITERAL FROM fromid=INTEGER_LITERAL TO toid=INTEGERLITERAL
 */
// TODO: Whether to support more complex migration, such as, migrate all region from 1, 2 to 5, 6
public class MigrateRegionStatement extends Statement implements IConfigStatement {

  private final int regionId;

  private final int fromId;

  private final int toId;

  public MigrateRegionStatement(int regionId, int fromId, int toId) {
    super();
    this.regionId = regionId;
    this.fromId = fromId;
    this.toId = toId;
  }

  public int getRegionId() {
    return regionId;
  }

  public int getFromId() {
    return fromId;
  }

  public int getToId() {
    return toId;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitMigrateRegion(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
