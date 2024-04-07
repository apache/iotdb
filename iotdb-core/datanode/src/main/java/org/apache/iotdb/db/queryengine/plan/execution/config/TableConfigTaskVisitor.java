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

package org.apache.iotdb.db.queryengine.plan.execution.config;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.UseDBTask;
import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.CreateDB;
import org.apache.iotdb.db.relational.sql.tree.CreateTable;
import org.apache.iotdb.db.relational.sql.tree.CurrentDatabase;
import org.apache.iotdb.db.relational.sql.tree.DescribeTable;
import org.apache.iotdb.db.relational.sql.tree.DropDB;
import org.apache.iotdb.db.relational.sql.tree.DropTable;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.ShowDB;
import org.apache.iotdb.db.relational.sql.tree.ShowTables;
import org.apache.iotdb.db.relational.sql.tree.Use;

public class TableConfigTaskVisitor extends AstVisitor<IConfigTask, MPPQueryContext> {

  private final IClientSession clientSession;

  public TableConfigTaskVisitor(IClientSession clientSession) {
    this.clientSession = clientSession;
  }

  @Override
  protected IConfigTask visitNode(Node node, MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  protected IConfigTask visitCreateDB(CreateDB node, MPPQueryContext context) {
    return super.visitCreateDB(node, context);
  }

  @Override
  protected IConfigTask visitUse(Use node, MPPQueryContext context) {
    return new UseDBTask(node, clientSession);
  }

  @Override
  protected IConfigTask visitDropDB(DropDB node, MPPQueryContext context) {
    return super.visitDropDB(node, context);
  }

  @Override
  protected IConfigTask visitShowDB(ShowDB node, MPPQueryContext context) {
    return super.visitShowDB(node, context);
  }

  @Override
  protected IConfigTask visitCreateTable(CreateTable node, MPPQueryContext context) {
    String currentDatabase = clientSession.getDatabaseName();
    // TODO using currentDatabase to normalize QualifiedName in CreateTable

    return new CreateTableTask(node);
  }

  @Override
  protected IConfigTask visitDropTable(DropTable node, MPPQueryContext context) {
    return super.visitDropTable(node, context);
  }

  @Override
  protected IConfigTask visitShowTables(ShowTables node, MPPQueryContext context) {
    return super.visitShowTables(node, context);
  }

  @Override
  protected IConfigTask visitDescribeTable(DescribeTable node, MPPQueryContext context) {
    return super.visitDescribeTable(node, context);
  }

  @Override
  protected IConfigTask visitCurrentDatabase(CurrentDatabase node, MPPQueryContext context) {
    return super.visitCurrentDatabase(node, context);
  }
}
