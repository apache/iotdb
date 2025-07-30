/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.schema;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.LbacOperationClassifier;
import org.apache.iotdb.db.auth.LbacPermissionChecker;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class NodeManageMemoryMergeOperator implements ProcessOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManageMemoryMergeOperator.class);

  private final OperatorContext operatorContext;
  private final Set<TSchemaNode> data;
  private final Set<String> nameSet;
  private final Operator child;
  private boolean isReadingMemory;
  private final List<TSDataType> outputDataTypes;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NodeManageMemoryMergeOperator.class);

  public NodeManageMemoryMergeOperator(
      OperatorContext operatorContext, Set<TSchemaNode> data, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.data = data;
    nameSet = data.stream().map(TSchemaNode::getNodeName).collect(Collectors.toSet());
    this.child = requireNonNull(child, "child operator is null");
    isReadingMemory = true;
    this.outputDataTypes =
        ColumnHeaderConstant.showChildPathsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return isReadingMemory ? NOT_BLOCKED : child.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    if (isReadingMemory) {
      isReadingMemory = false;
      return transferToTsBlock(data);
    } else {
      TsBlock block = child.nextWithTimer();
      if (block == null) {
        return null;
      }

      Set<TSchemaNode> nodePaths = new HashSet<>();
      for (int i = 0; i < block.getPositionCount(); i++) {
        TSchemaNode schemaNode =
            new TSchemaNode(
                block.getColumn(0).getBinary(i).toString(),
                Byte.parseByte(block.getColumn(1).getBinary(i).toString()));
        if (!nameSet.contains(schemaNode.getNodeName())) {
          nodePaths.add(schemaNode);
          nameSet.add(schemaNode.getNodeName());
        }
      }
      return transferToTsBlock(nodePaths);
    }
  }

  private TsBlock transferToTsBlock(Set<TSchemaNode> nodePaths) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    // sort by node type
    Set<TSchemaNode> sortSet =
        new TreeSet<>(
            (o1, o2) -> {
              if (o1.getNodeType() == o2.getNodeType()) {
                return o1.getNodeName().compareTo(o2.getNodeName());
              }
              return o1.getNodeType() - o2.getNodeType();
            });

    // Apply RBAC and LBAC filtering to nodes
    String userName = getCurrentUserName();
    for (TSchemaNode node : nodePaths) {
      if (hasPermissionForNode(userName, node)) {
        sortSet.add(node);
      }
    }

    sortSet.forEach(
        node -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder
              .getColumnBuilder(0)
              .writeBinary(new Binary(node.getNodeName(), TSFileConfig.STRING_CHARSET));
          tsBlockBuilder
              .getColumnBuilder(1)
              .writeBinary(
                  new Binary(
                      MNodeType.getMNodeType(node.getNodeType()).getNodeTypeName(),
                      TSFileConfig.STRING_CHARSET));
          tsBlockBuilder.declarePosition();
        });
    return tsBlockBuilder.build();
  }

  /**
   * Check if user has permission to access the schema node Combines RBAC and LBAC checks with LBAC
   * switch support
   */
  private boolean hasPermissionForNode(String userName, TSchemaNode node) {
    try {
      LOGGER.info("=== NODE PERMISSION CHECK START ===");
      LOGGER.info("User: {}, Node: {}", userName, node.getNodeName());

      // Super user has access to everything
      if (AuthorityChecker.SUPER_USER.equals(userName)) {
        LOGGER.info("Super user access granted");
        return true;
      }

      // Extract database path from node name
      String databasePath = extractDatabasePathFromNodeName(node.getNodeName());
      LOGGER.info("Extracted database path: {}", databasePath);

      if (databasePath == null) {
        // If we can't extract database path, allow access (or apply default policy)
        LOGGER.info("Cannot extract database path, allowing access");
        return true;
      }

      // Step 1: RBAC check - must pass first
      LOGGER.info("Performing RBAC check for database: {}", databasePath);
      boolean rbacAllowed =
          AuthorityChecker.checkFullPathOrPatternPermission(
              userName, new PartialPath(databasePath), PrivilegeType.READ_SCHEMA);

      if (!rbacAllowed) {
        LOGGER.warn(
            "User {} denied RBAC access to node {} in database {}",
            userName,
            node.getNodeName(),
            databasePath);
        return false;
      }

      LOGGER.info("RBAC check passed");

      // Step 2: LBAC check - only if LBAC is enabled
      if (LbacPermissionChecker.isLbacEnabled()) {
        LOGGER.info("LBAC is enabled, performing LBAC check");
        if (!LbacPermissionChecker.checkLbacPermissionForDatabase(
            userName, databasePath, LbacOperationClassifier.OperationType.READ)) {
          LOGGER.warn(
              "User {} denied LBAC access to node {} in database {}",
              userName,
              node.getNodeName(),
              databasePath);
          return false;
        }
        LOGGER.info("LBAC check passed");
      } else {
        LOGGER.info("LBAC is disabled, skipping LBAC check");
      }

      LOGGER.info("All permission checks passed for node: {}", node.getNodeName());
      return true;
    } catch (Exception e) {
      LOGGER.warn("Error checking permission for node {}: {}", node.getNodeName(), e.getMessage());
      // In case of error, deny access for security
      return false;
    }
  }

  /** Extract database path from node name For example: "root.database.device" -> "root.database" */
  private String extractDatabasePathFromNodeName(String nodeName) {
    if (nodeName == null || !nodeName.startsWith("root.")) {
      return null;
    }

    String[] parts = nodeName.split("\\.");
    if (parts.length >= 2) {
      return parts[0] + "." + parts[1]; // root.database
    }

    return null;
  }

  /** Get current user name from operator context */
  private String getCurrentUserName() {
    try {
      // Try to get user from session context
      // This is a simplified implementation - actual implementation may vary
      return operatorContext
          .getDriverContext()
          .getFragmentInstanceContext()
          .getSessionInfo()
          .getUserName();
    } catch (Exception e) {
      LOGGER.warn("Failed to get current user name, using 'unknown': {}", e.getMessage());
      return "unknown";
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return isReadingMemory || child.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !isReadingMemory && child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    // todo calculate the result based on all the scan node; currently, this is
    // shadowed by
    // schemaQueryMergeNode
    return Math.max(2L * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, child.calculateMaxPeekMemory());
  }

  @Override
  public long calculateMaxReturnSize() {
    return Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, child.calculateMaxReturnSize());
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child);
  }
}
