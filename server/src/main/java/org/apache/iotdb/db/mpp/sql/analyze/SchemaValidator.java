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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Arrays;

public class SchemaValidator {

  private static final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();

  public static boolean validate(InsertNode insertNode) {
    if (insertNode instanceof InsertRowNode) {
      return validate((InsertRowNode) insertNode);
    } else if (insertNode instanceof InsertTabletNode) {
      return validate((InsertTabletNode) insertNode);
    } else if (insertNode instanceof InsertRowsNode) {
      return validate((InsertRowsNode) insertNode);
    } else if (insertNode instanceof InsertMultiTabletsNode) {
      return validate((InsertMultiTabletsNode) insertNode);
    } else if (insertNode instanceof InsertRowsOfOneDeviceNode) {
      return validate((InsertRowsOfOneDeviceNode) insertNode);
    }

    // unsupported type
    return false;
  }

  private static boolean validate(InsertRowNode insertRowNode) {
    // InsertRowNode insertRowNode = (InsertRowNode) planNode;
    SchemaTree schemaTree =
        schemaFetcher.fetchSchemaWithAutoCreate(
            insertRowNode.getDevicePath(),
            insertRowNode.getMeasurements(),
            insertRowNode.getDataTypes(),
            insertRowNode.isAligned());

    try {
      insertRowNode.transferType(schemaTree);
    } catch (QueryProcessException e) {
      throw new SemanticException(e.getMessage());
    }

    if (!insertRowNode.checkDataType(schemaTree)) {
      throw new SemanticException("Data type mismatch");
    }

    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(
            insertRowNode.getDevicePath(), Arrays.asList(insertRowNode.getMeasurements()));
    insertRowNode.setMeasurementSchemas(
        deviceSchemaInfo.getMeasurementSchemaList().toArray(new MeasurementSchema[0]));
    return true;
  }

  private static boolean validate(InsertTabletNode insertTabletNode) {
    return true;
  }

  private static boolean validate(InsertRowsNode insertRowsNode) {
    return true;
  }

  private static boolean validate(InsertRowsOfOneDeviceNode insertRowsOfOneDeviceNode) {
    return true;
  }

  private static boolean validate(InsertMultiTabletsNode insertMultiTabletsNode) {
    return true;
  }
}
