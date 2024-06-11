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

package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropTable extends Statement {

  private final QualifiedName tableName;
  private final boolean exists;

  public DropTable(QualifiedName tableName, boolean exists) {
    super(null);
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.exists = exists;
  }

  public DropTable(NodeLocation location, QualifiedName tableName, boolean exists) {
    super(requireNonNull(location, "location is null"));
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.exists = exists;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropTable(this, context);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName, String databaseName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    String database = databaseName;
    if (databaseName == null) {
      if (this.tableName.getPrefix().isPresent()) {
        database = this.tableName.getPrefix().get().toString();
      }
    }
    if (database == null) {
      throw new SemanticException("unknown database");
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkDBPermision(userName, database, PrivilegeType.WRITE_SCHEMA.ordinal())
            || AuthorityChecker.checkTBPermission(
                userName, database, tableName.toString(), PrivilegeType.WRITE_SCHEMA.ordinal()),
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, exists);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropTable o = (DropTable) obj;
    return Objects.equals(tableName, o.tableName) && (exists == o.exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("tableName", tableName).add("exists", exists).toString();
  }
}
