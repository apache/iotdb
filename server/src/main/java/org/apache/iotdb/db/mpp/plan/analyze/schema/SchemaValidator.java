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

package org.apache.iotdb.db.mpp.plan.analyze.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.BatchInsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

public class SchemaValidator {

  private static final ISchemaFetcher SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();

  public static void validate(InsertNode insertNode) {
    try {
      if (insertNode instanceof BatchInsertNode) {
        SCHEMA_FETCHER.fetchAndComputeSchemaWithAutoCreate(
            ((BatchInsertNode) insertNode).getSchemaValidationList());
      } else {
        SCHEMA_FETCHER.fetchAndComputeSchemaWithAutoCreate(insertNode.getSchemaValidation());
      }
      insertNode.updateAfterSchemaValidation();
    } catch (QueryProcessException e) {
      throw new SemanticException(e);
    }
  }

  public static ISchemaTree validate(
      List<PartialPath> devicePaths,
      List<String[]> measurements,
      List<TSDataType[]> dataTypes,
      List<TSEncoding[]> encodings,
      List<CompressionType[]> compressionTypes,
      List<Boolean> isAlignedList) {
    return SCHEMA_FETCHER.fetchSchemaListWithAutoCreate(
        devicePaths, measurements, dataTypes, encodings, compressionTypes, isAlignedList);
  }
}
