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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Authority checker is SingleTon working at datanode.
// It checks permission in local. DCL statement will send to confignode.
public class AuthorityChecker {

  private static final String SUPER_USER =
      CommonDescriptor.getInstance().getConfig().getAdminName();

  private static IAuthorityFetcher authorityFetcher;

  private long heartBeatTimeStamp = 0;

  private static final CommonConfig config = CommonDescriptor.getInstance().getConfig();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  /** SingleTon. */
  private static class AuthorityCheckerHolder {
    private static final AuthorityChecker INSTANCE = new AuthorityChecker();

    private AuthorityCheckerHolder() {
      // Empty constructor
    }
  }

  public static AuthorityChecker getInstance() {
    return AuthorityChecker.AuthorityCheckerHolder.INSTANCE;
  }

  public AuthorityChecker() {
    authorityFetcher = new ClusterAuthorityFetcher(new BasicAuthorityCache());
  }

  // Authority checker will not check whether the paths or permission is legal.
  public static boolean checkFullPathListPermission(
      String username, List<PartialPath> paths, int permission) {
    long startTime = System.nanoTime();
    if (SUPER_USER.equals(username)) {
      return true;
    }
    TSStatus status = authorityFetcher.checkUserPathPrivileges(username, paths, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return true;
    }
    return false;
  }

  private static boolean checkFullPathPermission(
      String username, PartialPath fullPath, int permission) {
    long startTime = System.nanoTime();
    if (SUPER_USER.equals(username)) {
      return true;
    }
    List<PartialPath> path = new ArrayList<>();
    path.add(fullPath);
    TSStatus status = authorityFetcher.checkUserPathPrivileges(username, path, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return true;
    }
    return false;
  }

  public static PathPatternTree getAuthorizedPathTree(String username, int permission) {
    PathPatternTree pathTree = authorityFetcher.getAuthizedPatternTree(username, permission);
    return pathTree;
  }

  public static boolean checkSystemPermission(String username, int permission) {
    long startTime = System.nanoTime();
    if (SUPER_USER.equals(username)) {
      return true;
    }
    TSStatus status = authorityFetcher.checkUserSysPrivileges(username, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return true;
    }
    return false;
  }

  public boolean invalidateCache(String username, String roleName) {
    return authorityFetcher.getAuthorCache().invalidateCache(username, roleName);
  }

  public SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    return authorityFetcher.queryPermission(authorStatement);
  }

  public SettableFuture<ConfigTaskResult> operatePermission(AuthorStatement authorStatement) {
    return authorityFetcher.operatePermission(authorStatement);
  }

  public void refreshToken() {
    long currnetTime = System.currentTimeMillis();
    if (heartBeatTimeStamp == 0) {
      heartBeatTimeStamp = currnetTime;
      return;
    }
    if (currnetTime - heartBeatTimeStamp > config.getDatanodeTokenTimeoutMS()) {
      authorityFetcher.setCacheOutDate();
    }
  }

  public void buildTSBlock(
      Map<String, List<String>> authorizerInfo, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types = new ArrayList<>();
    for (int i = 0; i < authorizerInfo.size(); i++) {
      types.add(TSDataType.TEXT);
    }
    TsBlockBuilder builder = new TsBlockBuilder(types);
    List<ColumnHeader> headerList = new ArrayList<>();

    for (String header : authorizerInfo.keySet()) {
      headerList.add(new ColumnHeader(header, TSDataType.TEXT));
    }
    // The Time column will be ignored by the setting of ColumnHeader.
    // So we can put a meaningless value here
    for (String value : authorizerInfo.get(headerList.get(0).getColumnName())) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(value));
      builder.declarePosition();
    }
    for (int i = 1; i < headerList.size(); i++) {
      for (String value : authorizerInfo.get(headerList.get(i).getColumnName())) {
        builder.getColumnBuilder(i).writeBinary(new Binary(value));
      }
    }

    DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
