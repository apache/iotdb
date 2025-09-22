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

package org.apache.iotdb.db.queryengine.plan.statement.sys;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SetConfigurationStatement extends Statement implements IConfigStatement {

  private int nodeId;
  private Map<String, String> configItems;

  public SetConfigurationStatement(StatementType setConfigurationType) {
    this.statementType = setConfigurationType;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public void setConfigItems(Map<String, String> configItems) {
    this.configItems = configItems;
  }

  public int getNodeId() {
    return nodeId;
  }

  public Map<String, String> getConfigItems() {
    return configItems;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetConfiguration(this, context);
  }

  public Collection<PrivilegeType> getNeededPrivileges() throws IOException {
    Set<PrivilegeType> neededPrivileges = new HashSet<>();
    for (String key : this.getConfigItems().keySet()) {
      PrivilegeType neededPrivilege = ConfigurationFileUtils.getConfigurationItemPrivilege(key);
      if (neededPrivilege == null) {
        continue;
      }
      neededPrivileges.add(neededPrivilege);
    }
    return neededPrivileges;
  }

  public void checkSomeParametersKeepConsistentInCluster() {
    String specialParam = null;
    for (String key : this.getConfigItems().keySet()) {
      if (ConfigurationFileUtils.parameterNeedKeepConsistentInCluster(key)) {
        specialParam = key;
        break;
      }
    }
    if (specialParam == null) {
      return;
    }

    if (nodeId >= 0 || configItems.size() > 1) {
      throw new SemanticException(
          "The parameters '"
              + specialParam
              + "'  must be consistent across the entire cluster and only one can be set at a time.");
    }
  }
}
