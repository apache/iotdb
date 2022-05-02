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

package org.apache.iotdb.db.mpp.sql.statement.metadata;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class SetTTLStatement extends Statement implements IConfigStatement {
  private PartialPath storageGroupPath;
  private int ttl;

  public SetTTLStatement() {
    super();
    statementType = StatementType.TTL;
  }

  public PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  public int getTTL() {
    return ttl;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetTTL(this, context);
  }

  public void setStorageGroupPath(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  public void setTTL(int ttl) {
    this.ttl = ttl;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return storageGroupPath != null
        ? Collections.singletonList(storageGroupPath)
        : Collections.emptyList();
  }
}
