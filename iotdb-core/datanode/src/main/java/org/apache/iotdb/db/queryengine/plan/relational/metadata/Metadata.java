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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.db.queryengine.common.SessionInfo;

import java.util.Map;
import java.util.Optional;

public interface Metadata {

  boolean tableExists(QualifiedObjectName name);

  /**
   * Return table schema definition for the specified table handle. Table schema definition is a set
   * of information required by semantic analyzer to analyze the query.
   *
   * @throws RuntimeException if table handle is no longer valid
   * @see #getTableMetadata(SessionInfo, TableHandle)
   */
  TableSchema getTableSchema(SessionInfo session, TableHandle tableHandle);

  /**
   * Return the metadata for the specified table handle.
   *
   * @throws RuntimeException if table handle is no longer valid
   * @see #getTableSchema(SessionInfo, TableHandle) a different method which is less expensive.
   */
  TableMetadata getTableMetadata(SessionInfo session, TableHandle tableHandle);

  /** Returns a table handle for the specified table name with a specified version */
  Optional<TableHandle> getTableHandle(SessionInfo session, QualifiedObjectName name);

  /**
   * Gets all of the columns on the specified table, or an empty map if the columns cannot be
   * enumerated.
   *
   * @throws RuntimeException if table handle is no longer valid
   */
  Map<String, ColumnHandle> getColumnHandles(SessionInfo session, TableHandle tableHandle);
}
