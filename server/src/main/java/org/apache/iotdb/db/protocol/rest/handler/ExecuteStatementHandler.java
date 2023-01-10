/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;

public class ExecuteStatementHandler {
  private ExecuteStatementHandler() {}

  public static boolean validateStatement(Statement statement) {
    return !(statement instanceof QueryStatement)
        && !(statement instanceof ShowStatement
            && !(statement instanceof DropSchemaTemplateStatement))
        && !(statement instanceof AuthorStatement
            && (((AuthorStatement) statement)
                    .getAuthorType()
                    .name()
                    .equals(StatementType.LIST_USER.name())
                || ((AuthorStatement) statement)
                    .getAuthorType()
                    .name()
                    .equals(StatementType.LIST_ROLE.name())
                || ((AuthorStatement) statement)
                    .getAuthorType()
                    .name()
                    .equals(StatementType.LIST_USER_PRIVILEGE.name())
                || ((AuthorStatement) statement)
                    .getAuthorType()
                    .name()
                    .equals(StatementType.LIST_ROLE_PRIVILEGE.name())));
  }
}
