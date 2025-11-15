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
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.memory.MemoryException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.PreparedStatementInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Memory manager for PreparedStatement. All PreparedStatements from all sessions share the memory
 * pool allocated from CoordinatorMemoryManager. When memory is full, new PREPARE statements will
 * fail until some PreparedStatements are deallocated.
 */
public class PreparedStatementMemoryManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PreparedStatementMemoryManager.class);

  private static final PreparedStatementMemoryManager INSTANCE =
      new PreparedStatementMemoryManager();

  private PreparedStatementMemoryManager() {
    // singleton
  }

  public static PreparedStatementMemoryManager getInstance() {
    return INSTANCE;
  }

  /**
   * Allocate memory for a PreparedStatement.
   *
   * @param statementName the name of the prepared statement (used as memory block name)
   * @param memorySizeInBytes the memory size in bytes to allocate
   * @return the allocated memory block
   * @throws SemanticException if memory allocation fails
   */
  public IMemoryBlock allocate(String statementName, long memorySizeInBytes) {
    try {
      IMemoryBlock memoryBlock =
          IoTDBDescriptor.getInstance()
              .getMemoryConfig()
              .getCoordinatorMemoryManager()
              .exactAllocate(
                  "PreparedStatement-" + statementName, memorySizeInBytes, MemoryBlockType.DYNAMIC);

      LOGGER.debug(
          "Allocated {} bytes for PreparedStatement '{}'", memorySizeInBytes, statementName);
      return memoryBlock;
    } catch (MemoryException e) {
      LOGGER.warn(
          "Failed to allocate {} bytes for PreparedStatement '{}': {}",
          memorySizeInBytes,
          statementName,
          e.getMessage());
      throw new SemanticException(
          String.format(
              "Insufficient memory for PreparedStatement '%s'. "
                  + "Please deallocate some PreparedStatements and try again.",
              statementName));
    }
  }

  /**
   * Release memory for a PreparedStatement.
   *
   * @param memoryBlock the memory block to release
   */
  public void release(IMemoryBlock memoryBlock) {
    if (memoryBlock != null) {
      try {
        memoryBlock.close();
        LOGGER.debug(
            "Released memory block '{}' ({} bytes) for PreparedStatement",
            memoryBlock.getName(),
            memoryBlock.getTotalMemorySizeInBytes());
      } catch (Exception e) {
        LOGGER.error(
            "Failed to release memory block '{}' for PreparedStatement: {}",
            memoryBlock.getName(),
            e.getMessage(),
            e);
      }
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

    try {
      Set<String> preparedStatementNames = session.getPreparedStatementNames();
      if (preparedStatementNames == null || preparedStatementNames.isEmpty()) {
        return;
      }

      int releasedCount = 0;
      long totalReleasedBytes = 0;

      // Create a copy of the set to avoid concurrent modification
      Set<String> statementNamesCopy = new HashSet<>(preparedStatementNames);

      for (String statementName : statementNamesCopy) {
        try {
          PreparedStatementInfo info = session.getPreparedStatement(statementName);
          if (info != null && info.getMemoryBlock() != null) {
            IMemoryBlock memoryBlock = info.getMemoryBlock();
            if (!memoryBlock.isReleased()) {
              long memorySize = memoryBlock.getTotalMemorySizeInBytes();
              release(memoryBlock);
              releasedCount++;
              totalReleasedBytes += memorySize;
            }
          }
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to release PreparedStatement '{}' during session cleanup: {}",
              statementName,
              e.getMessage(),
              e);
        }
      }

      if (releasedCount > 0) {
        LOGGER.debug(
            "Released {} PreparedStatement(s) ({} bytes total) for session {}",
            releasedCount,
            totalReleasedBytes,
            session);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to release PreparedStatement resources for session {}: {}",
          session,
          e.getMessage(),
          e);
    }
  }
}
