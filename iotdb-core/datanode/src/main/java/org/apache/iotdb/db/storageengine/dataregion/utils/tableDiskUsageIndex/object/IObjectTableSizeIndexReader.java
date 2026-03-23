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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.object;

import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.DataRegionTableSizeQueryContext;

import java.io.IOException;

public interface IObjectTableSizeIndexReader extends AutoCloseable {
  /**
   * Load table size information from object index files into the given context.
   *
   * @param dataRegionContext the query context used to store table size results
   * @param startTime the start time of the current execution slice, in nanoseconds
   * @param maxRunTime the maximum allowed runtime for this execution slice, in nanoseconds
   * @return true if the loading process finishes within the given time budget, false if the
   *     execution should yield and continue in the next invocation
   * @throws IOException if an I/O error occurs while reading object files
   */
  boolean loadObjectFileTableSize(
      DataRegionTableSizeQueryContext dataRegionContext, long startTime, long maxRunTime)
      throws IOException;

  /**
   * Override {@link AutoCloseable#close()} to remove the checked exception from the method
   * signature. Implementations are not expected to throw any checked exception during close, which
   * simplifies the usage for callers.
   */
  @Override
  void close();
}
