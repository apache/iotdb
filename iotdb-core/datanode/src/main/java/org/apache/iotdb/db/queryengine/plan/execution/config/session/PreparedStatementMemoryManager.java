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

package org.apache.iotdb.db.queryengine.plan.execution.config.session;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.PreparedStatementInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Memory manager for PreparedStatement. All PreparedStatements from all sessions share a single
 * MemoryBlock named "Coordinator" allocated from CoordinatorMemoryManager. The MemoryBlock is
 * initialized in Coordinator with all available memory.
 */
public class PreparedStatementMemoryManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PreparedStatementMemoryManager.class);

  private static final PreparedStatementMemoryManager INSTANCE =
      new PreparedStatementMemoryManager();

  private static final String SHARED_MEMORY_BLOCK_NAME = "Coordinator";

  private PreparedStatementMemoryManager() {
    // singleton
  }

  public static PreparedStatementMemoryManager getInstance() {
    return INSTANCE;
  }

  private IMemoryBlock getSharedMemoryBlock() {
    return Coordinator.getCoordinatorMemoryBlock();
  }

  /**
   * Allocate memory for a PreparedStatement.
   *
   * @param statementName the name of the prepared statement
   * @param memorySizeInBytes the memory size in bytes to allocate
   * @throws SemanticException if memory allocation fails
   */
  public void allocate(String statementName, long memorySizeInBytes) {
    IMemoryBlock sharedMemoryBlock = getSharedMemoryBlock();
    // Allocate memory from the shared block
    boolean allocated = sharedMemoryBlock.allocate(memorySizeInBytes);
    if (!allocated) {
      LOGGER.warn(
          "Failed to allocate {} bytes from shared MemoryBlock '{}' for PreparedStatement '{}'",
          memorySizeInBytes,
          SHARED_MEMORY_BLOCK_NAME,
          statementName);
      throw new SemanticException(
          String.format(
              "Insufficient memory for PreparedStatement '%s'. "
                  + "Please deallocate some PreparedStatements and try again.",
              statementName));
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Allocated {} bytes for PreparedStatement '{}' from shared MemoryBlock '{}'. ",
          memorySizeInBytes,
          statementName,
          SHARED_MEMORY_BLOCK_NAME);
    }
  }

  /**
   * Release memory for a PreparedStatement.
   *
   * @param memorySizeInBytes the memory size in bytes to release
   */
  public void release(long memorySizeInBytes) {
    if (memorySizeInBytes <= 0) {
      return;
    }

    IMemoryBlock sharedMemoryBlock = getSharedMemoryBlock();
    if (!sharedMemoryBlock.isReleased()) {
      long releasedSize = sharedMemoryBlock.release(memorySizeInBytes);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Released {} bytes from shared MemoryBlock '{}' for PreparedStatement. ",
            releasedSize,
            SHARED_MEMORY_BLOCK_NAME);
      }
    } else {
      LOGGER.error(
          "Attempted to release memory from shared MemoryBlock '{}' but it is released",
          SHARED_MEMORY_BLOCK_NAME);
    }
  }

  /**
   * Release all PreparedStatements for a session. This method should be called when a session is
   * closed or connection is lost.
   *
   * @param session the session whose PreparedStatements should be released
   */
  public void releaseAllForSession(IClientSession session) {
    if (session == null) {
      return;
    }

    Set<String> preparedStatementNames = session.getPreparedStatementNames();
    if (preparedStatementNames == null || preparedStatementNames.isEmpty()) {
      return;
    }

    int releasedCount = 0;
    long totalReleasedBytes = 0;

    for (String statementName : preparedStatementNames) {
      PreparedStatementInfo info = session.getPreparedStatement(statementName);
      if (info != null) {
        long memorySize = info.getMemorySizeInBytes();
        if (memorySize > 0) {
          release(memorySize);
          releasedCount++;
          totalReleasedBytes += memorySize;
        }
      }
    }

    if (releasedCount > 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Released {} PreparedStatement(s) ({} bytes total) for session {}",
          releasedCount,
          totalReleasedBytes,
          session);
    }
  }
}
