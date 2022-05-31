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
package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandaloneSchemaFetcher implements ISchemaFetcher {

  private final LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();

  private StandaloneSchemaFetcher() {}

  public static StandaloneSchemaFetcher getInstance() {
    return new StandaloneSchemaFetcher();
  }

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree) {
    Set<String> storageGroupSet = new HashSet<>();
    SchemaTree schemaTree = new SchemaTree();
    List<PartialPath> partialPathList = patternTree.splitToPathList();
    try {
      for (PartialPath path : partialPathList) {
        String storageGroup = localConfigNode.getBelongedStorageGroup(path).getFullPath();
        storageGroupSet.add(storageGroup);
        SchemaRegionId schemaRegionId = localConfigNode.getBelongedSchemaRegionId(path);
        ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
        schemaTree.appendMeasurementPaths(schemaRegion.getMeasurementPaths(path, false));
      }
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
    schemaTree.setStorageGroups(new ArrayList<>(storageGroupSet));
    return schemaTree;
  }

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree, SchemaPartition schemaPartition) {
    return null;
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean aligned) {
    return null;
  }

  @Override
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath,
      List<String[]> measurements,
      List<TSDataType[]> tsDataTypes,
      List<Boolean> aligned) {
    Map<PartialPath, List<String>> deviceToMeasurementMap = new HashMap<>();
    for (int i = 0; i < devicePath.size(); i++) {
      deviceToMeasurementMap.put(devicePath.get(i), Arrays.asList(measurements.get(i)));
    }
    // todo implement auto create schema
    return fetchSchema(new PathPatternTree(deviceToMeasurementMap));
  }

  @Override
  public void invalidAllCache() {}
}
