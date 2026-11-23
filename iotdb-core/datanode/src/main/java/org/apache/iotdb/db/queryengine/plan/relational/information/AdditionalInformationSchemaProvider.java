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

package org.apache.iotdb.db.queryengine.plan.relational.information;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;

/** Provides additional content and execution locations for information schema tables. */
public interface AdditionalInformationSchemaProvider {

  /**
   * Returns the content supplier for the table, or {@code null} if the table is not handled.
   *
   * @throws Exception if the content supplier cannot be created.
   */
  default IInformationSchemaContentSupplier getContentSupplier(
      final String tableName,
      final OperatorContext context,
      final List<TSDataType> dataTypes,
      final UserEntity userEntity,
      final InformationSchemaTableScanNode node)
      throws Exception {
    return null;
  }

  /** Returns the execution location for the table, or {@code null} if the table is not handled. */
  default List<TDataNodeLocation> getTableLocation(final String tableName) {
    return null;
  }

  enum InformationSchemaTableLocation {
    LOCAL_DATA_NODE,
    ALL_READABLE_DATA_NODES
  }
}
