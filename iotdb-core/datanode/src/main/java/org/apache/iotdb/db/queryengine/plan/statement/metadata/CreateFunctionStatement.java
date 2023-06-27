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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class CreateFunctionStatement extends Statement implements IConfigStatement {

  private final String udfName;
  private final String className;

  private String uriString;

  private final boolean usingURI;

  public CreateFunctionStatement(String udfName, String className, boolean usingURI) {
    super();
    statementType = StatementType.CREATE_FUNCTION;
    this.udfName = udfName;
    this.className = className;
    this.usingURI = usingURI;
  }

  public CreateFunctionStatement(
      String udfName, String className, boolean usingURI, String uriString) {
    super();
    statementType = StatementType.CREATE_FUNCTION;
    this.udfName = udfName;
    this.className = className;
    this.usingURI = usingURI;
    this.uriString = uriString;
  }

  public String getUdfName() {
    return udfName;
  }

  public String getClassName() {
    return className;
  }

  public String getUriString() {
    return uriString;
  }

  public boolean isUsingURI() {
    return usingURI;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateFunction(this, context);
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
