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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * COUNT statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>COUNT {DATABASES | DEVICES | TIMESERIES | NODES} [prefixPath] [GROUP BY] LEVEL = level
 */
public class CountStatement extends ShowStatement {
  protected PartialPath pathPattern;
  private boolean canSeeSystemDB = true;

  public CountStatement(PartialPath pathPattern) {
    this.pathPattern = pathPattern;
    setType(StatementType.COUNT);
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public void setPathPattern(PartialPath pathPattern) {
    this.pathPattern = pathPattern;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      setCanSeeSystemDB(true);
      return AuthorityChecker.SUCCEED;
    }
    TSStatus explicitInternalDatabaseStatus = checkExplicitInternalDatabase(userName);
    if (explicitInternalDatabaseStatus != null) {
      return explicitInternalDatabaseStatus;
    }
    setCanSeeSystemDB(userName);
    TSStatus status = super.checkPermissionBeforeProcess(userName);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      appendInternalDatabaseAuthorityScope();
    }
    return status;
  }

  public boolean isCanSeeSystemDB() {
    return canSeeSystemDB;
  }

  public void setCanSeeSystemDB(boolean canSeeSystemDB) {
    this.canSeeSystemDB = canSeeSystemDB;
  }

  protected void setCanSeeSystemDB(String userName) {
    setCanSeeSystemDB(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MAINTAIN.ordinal()));
  }

  protected TSStatus checkExplicitInternalDatabase(String userName) {
    if (!isExplicitSystemDatabasePath()) {
      return null;
    }
    if (!AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MAINTAIN.ordinal())) {
      return AuthorityChecker.getTSStatus(false, PrivilegeType.MAINTAIN);
    }
    setCanSeeSystemDB(true);
    authorityScope = createAuthorityScope(pathPattern);
    return AuthorityChecker.SUCCEED;
  }

  protected void appendInternalDatabaseAuthorityScope() {
    if (!canSeeSystemDB || SchemaConstant.ALL_MATCH_SCOPE.equals(authorityScope)) {
      return;
    }
    authorityScope.appendPathPattern(createInternalDatabasePathPattern());
    authorityScope.constructTree();
  }

  private boolean isExplicitSystemDatabasePath() {
    String[] nodes = pathPattern.getNodes();
    return nodes.length >= 2
        && SchemaConstant.ROOT.equals(nodes[0])
        && SchemaConstant.SYSTEM_DATABASE.equals(SchemaConstant.ROOT + "." + nodes[1]);
  }

  private PartialPath createInternalDatabasePathPattern() {
    String[] databaseNodes = SchemaConstant.SYSTEM_DATABASE.split("\\.");
    String[] pathPatternNodes = Arrays.copyOf(databaseNodes, databaseNodes.length + 1);
    pathPatternNodes[databaseNodes.length] = IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
    return new PartialPath(pathPatternNodes);
  }

  private PathPatternTree createAuthorityScope(PartialPath pathPattern) {
    PathPatternTree authorityScope = new PathPatternTree();
    authorityScope.appendPathPattern(pathPattern);
    authorityScope.constructTree();
    return authorityScope;
  }
}
