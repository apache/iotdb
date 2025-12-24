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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;

/**
 * This interface is used to fetch the metadata information required in execution plan generating.
 */
public interface ISchemaFetcher {

  /**
   * Fetch all the schema of existing timeseries matched by the given patternTree. The result schema
   * tree may contain extra measurement nodes if withTemplate is true. It recommends to use pattern
   * to filter the result schema tree.
   *
   * @param patternTree used for matching the timeseries
   * @param withTemplate whether to include the template in the schema
   * @return the matched timeseries schema organized as tree structure logically
   */
  ISchemaTree fetchSchema(
      PathPatternTree patternTree,
      boolean withTemplate,
      MPPQueryContext context,
      boolean canSeeAuditDB);

  /**
   * Fetch all the schema by the given patternTree in device level
   *
   * @return schemaTree without measurement nodes
   */
  ISchemaTree fetchRawSchemaInDeviceLevel(
      PathPatternTree patternTree,
      PathPatternTree authorityScope,
      MPPQueryContext context,
      boolean canSeeAuditDB);

  /**
   * Fetch all the schema by the given patternTree in device level
   *
   * @return schemaTree without measurement nodes
   */
  ISchemaTree fetchRawSchemaInMeasurementLevel(
      PathPatternTree patternTree,
      PathPatternTree authorityScope,
      MPPQueryContext context,
      boolean canSeeAuditDB);

  /**
   * Fetch all the schema with tags of existing timeseries matched by the given patternTree
   *
   * @param patternTree used for matching the timeseries
   * @return the matched timeseries schema organized as tree structure logically
   */
  ISchemaTree fetchSchemaWithTags(
      PathPatternTree patternTree,
      boolean withTemplate,
      MPPQueryContext context,
      boolean canSeeAuditDB);

  /**
   * Fetch and compute the schema of target timeseries, with device and measurement defined in given
   * schemaComputationWithAutoCreation. The computation defined in given
   * schemaComputationWithAutoCreation will be executed during scanning the fetched schema. If some
   * target timeseries doesn't exist, they will be auto created.
   *
   * @param schemaComputationWithAutoCreation define the target device, measurements and computation
   */
  void fetchAndComputeSchemaWithAutoCreate(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation,
      MPPQueryContext context);

  /**
   * Fetch and compute the schema of target timeseries, with device and measurement defined in given
   * schemaComputationWithAutoCreation. The computation defined in given
   * schemaComputationWithAutoCreation will be executed during scanning the fetched schema. If some
   * target timeseries doesn't exist, they will be auto created.
   *
   * @param schemaComputationWithAutoCreationList define the target devices, measurements and
   *     computation
   */
  void fetchAndComputeSchemaWithAutoCreate(
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList,
      MPPQueryContext context);

  ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath,
      List<String[]> measurements,
      List<TSDataType[]> tsDataTypes,
      List<TSEncoding[]> encodings,
      List<CompressionType[]> compressionTypes,
      List<Boolean> aligned,
      MPPQueryContext context);

  Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath devicePath);

  Pair<Template, PartialPath> checkTemplateSetAndPreSetInfo(
      PartialPath timeSeriesPath, String alias);

  Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern);

  Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName);
}
