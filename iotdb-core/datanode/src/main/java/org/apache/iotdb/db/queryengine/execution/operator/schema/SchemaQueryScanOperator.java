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

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.auth.LbacOperationClassifier;
import org.apache.iotdb.db.auth.LbacPermissionChecker;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.queryengine.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ISchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class SchemaQueryScanOperator<T extends ISchemaInfo> implements SourceOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaQueryScanOperator.class);
  private static final long MAX_SIZE =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaQueryScanOperator.class);

  protected PlanNodeId sourceId;

  protected OperatorContext operatorContext;

  private final ISchemaSource<T> schemaSource;

  private long limit = -1;
  private long offset = 0;

  private String database;

  private final List<TSDataType> outputDataTypes;

  private ISchemaReader<T> schemaReader;

  private final TsBlockBuilder tsBlockBuilder;
  private ListenableFuture<?> isBlocked;
  private TsBlock next;
  private boolean isFinished;
  private long count = 0;

  public SchemaQueryScanOperator(
      final PlanNodeId sourceId,
      final OperatorContext operatorContext,
      final ISchemaSource<T> schemaSource) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.schemaSource = schemaSource;
    this.outputDataTypes =
        schemaSource.getInfoQueryColumnHeaders().stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  protected ISchemaReader<T> createSchemaReader() {
    return schemaSource.getSchemaReader(
        ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion());
  }

  private void setColumns(final T element, final TsBlockBuilder builder) {
    schemaSource.transformToTsBlockColumns(element, builder, getDatabase());
  }

  public void setLimit(final long limit) {
    this.limit = limit;
  }

  public void setOffset(final long offset) {
    this.offset = offset;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (isBlocked == null) {
      isBlocked = tryGetNext();
    }
    return isBlocked;
  }

  /**
   * Try to get next {@link TsBlock}. If the next is not ready, return a future. After success,
   * {@link SchemaQueryScanOperator#next} will be set.
   */
  private ListenableFuture<?> tryGetNext() {
    LOGGER.info("=== SCHEMA QUERY SCAN OPERATOR TRY GET NEXT START ===");

    if (schemaReader == null) {
      schemaReader = createSchemaReader();
      LOGGER.info("Created schema reader: {}", schemaReader);
    }

    while (true) {
      try {
        final ListenableFuture<?> readerBlocked = schemaReader.isBlocked();
        if (!readerBlocked.isDone()) {
          LOGGER.info("Schema reader is blocked, waiting...");
          final SettableFuture<?> settableFuture = SettableFuture.create();
          readerBlocked.addListener(
              () -> {
                next = tsBlockBuilder.build();
                tsBlockBuilder.reset();
                settableFuture.set(null);
              },
              directExecutor());
          return settableFuture;
        } else if (schemaReader.hasNext() && (limit < 0 || count < offset + limit)) {
          final T element = schemaReader.next();
          LOGGER.info("Got schema element: {}", element);
          LOGGER.info("Count: {}, Offset: {}, Limit: {}", count, offset, limit);

          if (++count > offset) {
            LOGGER.info("Processing element (count > offset), checking permissions");
            // Check permissions for schema info before including it in results
            if (hasPermissionForSchemaInfo(element)) {
              LOGGER.info("Permission check passed, adding element to result");
              setColumns(element, tsBlockBuilder);
              if (tsBlockBuilder.getRetainedSizeInBytes() >= MAX_SIZE) {
                next = tsBlockBuilder.build();
                tsBlockBuilder.reset();
                LOGGER.info("TsBlock reached max size, returning");
                return NOT_BLOCKED;
              }
            } else {
              LOGGER.info("Permission check failed, skipping element");
            }
            // If permission check fails, we skip this element but continue processing
          } else {
            LOGGER.info("Skipping element (count <= offset)");
          }
        } else {
          LOGGER.info("No more data from schema reader or limit reached");
          LOGGER.info(
              "Schema reader has next: {}, Limit: {}, Count: {}, Offset: {}",
              schemaReader.hasNext(),
              limit,
              count,
              offset);

          if (tsBlockBuilder.isEmpty()) {
            next = null;
            isFinished = true;
            LOGGER.info("TsBlockBuilder is empty, setting finished to true");
          } else {
            next = tsBlockBuilder.build();
            LOGGER.info("Building final TsBlock with {} rows", next.getPositionCount());
          }
          tsBlockBuilder.reset();
          LOGGER.info("=== SCHEMA QUERY SCAN OPERATOR TRY GET NEXT END ===");
          return NOT_BLOCKED;
        }
      } catch (final Exception e) {
        LOGGER.error("Error in tryGetNext: {}", e.getMessage(), e);
        throw new SchemaExecutionException(e);
      }
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final TsBlock ret = next;
    next = null;
    isBlocked = null;
    return ret;
  }

  @Override
  public boolean hasNext() throws Exception {
    isBlocked().get(); // wait for the next TsBlock
    if (!schemaReader.isSuccess()) {
      throw new SchemaExecutionException(schemaReader.getFailure());
    }
    return next != null;
  }

  @Override
  public boolean isFinished() throws Exception {
    return isFinished;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return schemaSource.getMaxMemory(getSchemaRegion());
  }

  @Override
  public long calculateMaxReturnSize() {
    return schemaSource.getMaxMemory(getSchemaRegion());
  }

  private ISchemaRegion getSchemaRegion() {
    return ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  protected String getDatabase() {
    if (database == null) {
      database =
          ((SchemaDriverContext) operatorContext.getDriverContext())
              .getSchemaRegion()
              .getDatabaseFullPath();
    }
    return database;
  }

  @Override
  public void close() throws Exception {
    if (schemaReader != null) {
      schemaReader.close();
      schemaReader = null;
    }
  }

  /**
   * Check if user has permission to access the schema info Only performs LBAC check since RBAC is
   * handled by Statement layer through authorityScope
   */
  private boolean hasPermissionForSchemaInfo(T schemaInfo) {
    try {
      LOGGER.info("=== SCHEMA INFO PERMISSION CHECK START ===");
      String userName = getCurrentUserName();
      LOGGER.info("User: {}, SchemaInfo: {}", userName, schemaInfo);

      // Extract database path from schema info
      String databasePath = extractDatabasePathFromSchemaInfo(schemaInfo);
      LOGGER.info("Extracted database path: {}", databasePath);

      if (databasePath == null) {
        // If we can't extract database path, allow access (or apply default policy)
        LOGGER.info("Cannot extract database path, allowing access");
        return true;
      }

      // Only perform LBAC check since RBAC is handled by Statement layer
      if (LbacPermissionChecker.isLbacEnabled()) {
        LOGGER.info("LBAC is enabled, performing LBAC check");
        if (!LbacPermissionChecker.checkLbacPermissionForDatabase(
            userName, databasePath, LbacOperationClassifier.OperationType.READ)) {
          LOGGER.warn(
              "User {} denied LBAC access to schema info in database {}", userName, databasePath);
          return false;
        }
        LOGGER.info("LBAC check passed");
      } else {
        LOGGER.info("LBAC is disabled, skipping LBAC check");
      }

      LOGGER.info("All permission checks passed for schema info");
      return true;
    } catch (Exception e) {
      LOGGER.warn("Error checking permission for schema info: {}", e.getMessage());
      // In case of error, deny access for security
      return false;
    }
  }

  /**
   * Extract database path from schema info This method handles different types of schema
   * information
   */
  private String extractDatabasePathFromSchemaInfo(T schemaInfo) {
    try {
      // Try to get path information from schema info
      // This implementation depends on the specific ISchemaInfo type
      String fullPath = schemaInfo.toString(); // Simplified approach

      if (fullPath != null && fullPath.startsWith("root.")) {
        String[] parts = fullPath.split("\\.");
        if (parts.length >= 2) {
          return parts[0] + "." + parts[1]; // root.database
        }
      }

      // Fallback: use current database from context
      return getDatabase();
    } catch (Exception e) {
      LOGGER.debug("Error extracting database path from schema info: {}", e.getMessage());
      return getDatabase(); // Fallback to current database
    }
  }

  /** Get current user name from operator context */
  private String getCurrentUserName() {
    try {
      // Try to get user from session context
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
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + RamUsageEstimator.sizeOf(database);
  }
}
