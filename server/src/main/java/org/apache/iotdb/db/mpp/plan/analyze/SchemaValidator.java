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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.BatchInsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;

public class SchemaValidator {

  private static final ISchemaFetcher SCHEMA_FETCHER =
      IoTDBDescriptor.getInstance().getConfig().isClusterMode()
          ? ClusterSchemaFetcher.getInstance()
          : StandaloneSchemaFetcher.getInstance();

  public static ISchemaTree validate(InsertNode insertNode) {

    ISchemaTree schemaTree;
    if (insertNode instanceof BatchInsertNode) {
      BatchInsertNode batchInsertNode = (BatchInsertNode) insertNode;
      schemaTree =
          SCHEMA_FETCHER.fetchSchemaListWithAutoCreate(
              batchInsertNode.getDevicePaths(),
              batchInsertNode.getMeasurementsList(),
              batchInsertNode.getDataTypesList(),
              batchInsertNode.getAlignedList());
    } else {
      schemaTree =
          SCHEMA_FETCHER.fetchSchemaWithAutoCreate(
              insertNode.getDevicePath(),
              insertNode.getMeasurements(),
              insertNode.getDataTypes(),
              insertNode.isAligned());
    }

    if (!insertNode.validateAndSetSchema(schemaTree)) {
      throw new SemanticException("Data type mismatch");
    }

    return schemaTree;
  }
}
