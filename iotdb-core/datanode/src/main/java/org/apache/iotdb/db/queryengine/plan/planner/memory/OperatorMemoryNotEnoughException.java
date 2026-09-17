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

package org.apache.iotdb.db.queryengine.plan.planner.memory;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;

/** Preserves the failed batch size and available memory for query-analysis diagnostics. */
public class OperatorMemoryNotEnoughException extends MemoryNotEnoughException {

  private final long requestedBytes;
  private final long freeBytes;

  public OperatorMemoryNotEnoughException(String message, long requestedBytes, long freeBytes) {
    super(message);
    this.requestedBytes = requestedBytes;
    this.freeBytes = freeBytes;
  }

  public long getRequestedBytes() {
    return requestedBytes;
  }

  public long getFreeBytes() {
    return freeBytes;
  }
}
