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
package org.apache.iotdb.db.queryengine.plan.statement;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;

import java.util.List;

public abstract class AuthorityInformationStatement extends Statement {
  protected List<PathPatternTree> authorityTreeList;

  public PathPatternTree getAuthorityTree(int index) {
    assert authorityTreeList != null;
    return authorityTreeList.get(index);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    this.authorityTreeList =
        ImmutableList.of(
            AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA.ordinal()));
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
