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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
