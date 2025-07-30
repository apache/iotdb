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
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * SHOW TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW [LATEST] TIMESERIES [pathPattern] [WHERE key { = | CONTAINS } value] [LIMIT limit]
 * [OFFSET offset]
 */
public class ShowTimeSeriesStatement extends ShowStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowTimeSeriesStatement.class);

  private final PartialPath pathPattern;
  private SchemaFilter schemaFilter;
  // if is true, the result will be sorted according to the inserting frequency of the time series
  private final boolean orderByHeat;
  private WhereCondition timeCondition;

  public ShowTimeSeriesStatement(PartialPath pathPattern, boolean orderByHeat) {
    super();
    this.pathPattern = pathPattern;
    this.orderByHeat = orderByHeat;
    LOGGER.info(
        "ShowTimeSeriesStatement created with pathPattern: {}, orderByHeat: {}",
        pathPattern,
        orderByHeat);
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public void setTimeCondition(WhereCondition timeCondition) {
    this.timeCondition = timeCondition;
  }

  public WhereCondition getTimeCondition() {
    return timeCondition;
  }

  public boolean hasTimeCondition() {
    return timeCondition != null;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    LOGGER.info("=== SHOW TIMESERIES PERMISSION CHECK START ===");
    LOGGER.info(
        "User: {}, PathPattern: {}, HasTimeCondition: {}",
        userName,
        pathPattern,
        hasTimeCondition());

    if (hasTimeCondition()) {
      LOGGER.info("ShowTimeSeriesStatement has time condition, using special permission check");
      try {
        if (!AuthorityChecker.SUPER_USER.equals(userName)) {
          LOGGER.info("User is not super user, getting authorized path tree");
          this.authorityScope =
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_DATA));
          LOGGER.info("Authority scope set: {}", authorityScope);
        } else {
          LOGGER.info("User is super user, bypassing permission check");
        }
      } catch (AuthException e) {
        LOGGER.error("AuthException during permission check: {}", e.getMessage(), e);
        return new TSStatus(e.getCode().getStatusCode());
      }
      LOGGER.info("ShowTimeSeriesStatement permission check completed successfully");
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      LOGGER.info(
          "ShowTimeSeriesStatement has no time condition, using super.checkPermissionBeforeProcess");
      TSStatus result = super.checkPermissionBeforeProcess(userName);
      LOGGER.info("Super permission check result: {}", result);
      return result;
    }
  }

  @Override
  public List<PartialPath> getPaths() {
    LOGGER.debug(
        "ShowTimeSeriesStatement.getPaths() called, returning: {}",
        Collections.singletonList(pathPattern));
    return Collections.singletonList(pathPattern);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTimeSeries(this, context);
  }
}
