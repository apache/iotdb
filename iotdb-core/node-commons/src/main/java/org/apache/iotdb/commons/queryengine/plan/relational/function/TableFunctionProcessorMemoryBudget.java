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

package org.apache.iotdb.commons.queryengine.plan.relational.function;

/**
 * Engine-internal memory accounting for the processors of a built-in table function.
 *
 * <p>It is deliberately kept out of {@code udf-api}: a user-defined table function does not declare
 * any buffered bytes, and the engine treats every provider that does not implement this interface
 * as keeping no state across calls. The value is resolved once per provider, and the processors
 * created by that provider must enforce exactly the same limit, so the estimate the operator
 * reserves and the limit the processors apply cannot diverge, not even when the configuration is
 * hot-reloaded while a query runs.
 */
public interface TableFunctionProcessorMemoryBudget {

  /**
   * Bytes the processors of this provider may buffer internally while one partition is processed,
   * above what {@code process} returns.
   */
  long getEstimatedMaxBufferedBytes();
}
