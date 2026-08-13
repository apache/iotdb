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

package org.apache.iotdb.calc.utils.sort;

import org.apache.iotdb.commons.exception.IoTDBException;

/**
 * Hook for per-user TEMP_DISK quota accounting on query spill (external sort). calc-commons cannot
 * depend on the DataNode quota manager, so each {@link DiskSpiller} receives an implementation via
 * {@link
 * org.apache.iotdb.calc.execution.operator.CommonOperatorContext#getTempDiskSpillQuotaGate()}.
 * TEMP_DISK is charged only for bytes actually written to spill files, never pre-allocated at query
 * admission.
 */
public interface TempDiskSpillQuotaGate {

  /**
   * Charge {@code bytes} of temporary disk usage against {@code userId} before writing them.
   *
   * @throws IoTDBException when the user's TEMP_DISK quota or the node capacity is exceeded
   */
  void acquire(long userId, long bytes) throws IoTDBException;

  /** Return previously charged bytes when the spill files are no longer referenced. */
  void release(long userId, long bytes);
}
