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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DeleteStorageGroupStatement extends Statement implements IConfigStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteStorageGroupStatement.class);

  private List<String> prefixPathList;

  public DeleteStorageGroupStatement() {
    super();
    statementType = StatementType.DELETE_STORAGE_GROUP;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> paths = new ArrayList<>();
    for (String prefixPath : prefixPathList) {
      try {
        paths.add(new PartialPath(prefixPath));
      } catch (IllegalPathException e) {
        LOGGER.error("{} is not a legal path", prefixPath, e);
      }
    }
    return paths;
  }

  public List<String> getPrefixPath() {
    return prefixPathList;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitDeleteStorageGroup(this, context);
  }

  public void setPrefixPath(List<String> prefixPathList) {
    this.prefixPathList = prefixPathList;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
