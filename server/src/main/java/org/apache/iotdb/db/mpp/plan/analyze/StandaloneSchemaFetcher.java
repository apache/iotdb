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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.DeviceGroupSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class StandaloneSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();

  private StandaloneSchemaFetcher() {}

  public static StandaloneSchemaFetcher getInstance() {
    return new StandaloneSchemaFetcher();
  }

  @Override
  public ClusterSchemaTree fetchSchema(PathPatternTree patternTree) {
    return fetchSchema(patternTree, false);
  }

  @Override
  public ClusterSchemaTree fetchSchemaWithTags(PathPatternTree patternTree) {
    return fetchSchema(patternTree, true);
  }

  private ClusterSchemaTree fetchSchema(PathPatternTree patternTree, boolean withTags) {
    patternTree.constructTree();
    Set<String> storageGroupSet = new HashSet<>();
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    List<PartialPath> pathPatterns = patternTree.getAllPathPatterns();
    try {
      for (PartialPath pathPattern : pathPatterns) {
        List<PartialPath> storageGroups = localConfigNode.getBelongedStorageGroups(pathPattern);
        for (PartialPath storageGroupPath : storageGroups) {
          storageGroupSet.add(storageGroupPath.getFullPath());
          SchemaRegionId schemaRegionId =
              localConfigNode.getBelongedSchemaRegionId(storageGroupPath);
          ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
          schemaTree.appendMeasurementPaths(
              schemaRegion.getMeasurementPaths(pathPattern, false, withTags));
        }
      }
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
    schemaTree.setDatabases(storageGroupSet);
    return schemaTree;
  }

  @Override
  public ISchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      boolean aligned) {
    DeviceSchemaInfo deviceSchemaInfo =
        getDeviceSchemaInfoWithAutoCreate(
            devicePath,
            measurements,
            getDataType,
            new TSEncoding[measurements.length],
            new CompressionType[measurements.length],
            aligned);
    DeviceGroupSchemaTree schemaTree = new DeviceGroupSchemaTree();
    schemaTree.addDeviceInfo(deviceSchemaInfo);
    return schemaTree;
  }

  private DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
      boolean aligned) {
    try {
      SchemaRegionId schemaRegionId = localConfigNode.getBelongedSchemaRegionId(devicePath);
      ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
      return schemaRegion.getDeviceSchemaInfoWithAutoCreate(
          devicePath, measurements, getDataType, encodings, compressionTypes, aligned);
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {
    return fetchSchemaListWithAutoCreate(
        devicePathList, measurementsList, tsDataTypesList, null, null, isAlignedList);
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<TSEncoding[]> encodingsList,
      List<CompressionType[]> compressionTypesList,
      List<Boolean> isAlignedList) {
    Map<PartialPath, List<Integer>> deviceMap = new HashMap<>();
    for (int i = 0, size = devicePathList.size(); i < size; i++) {
      deviceMap.computeIfAbsent(devicePathList.get(i), k -> new ArrayList<>()).add(i);
    }

    DeviceGroupSchemaTree schemaTree = new DeviceGroupSchemaTree();

    for (Map.Entry<PartialPath, List<Integer>> entry : deviceMap.entrySet()) {
      int totalSize = 0;
      boolean isAligned = isAlignedList.get(entry.getValue().get(0));
      for (int index : entry.getValue()) {
        if (isAlignedList.get(index) != isAligned) {
          throw new StatementAnalyzeException(
              String.format("Inconsistent device alignment of %s in insert plan.", entry.getKey()));
        }
        totalSize += measurementsList.get(index).length;
      }

      String[] measurements = new String[totalSize];
      TSDataType[] tsDataTypes = new TSDataType[totalSize];
      TSEncoding[] encodings = new TSEncoding[totalSize];
      CompressionType[] compressionTypes = new CompressionType[totalSize];

      int curPos = 0;
      for (int index : entry.getValue()) {
        System.arraycopy(
            measurementsList.get(index),
            0,
            measurements,
            curPos,
            measurementsList.get(index).length);
        System.arraycopy(
            tsDataTypesList.get(index), 0, tsDataTypes, curPos, tsDataTypesList.get(index).length);
        if (encodingsList != null) {
          System.arraycopy(
              encodingsList.get(index), 0, encodings, curPos, encodingsList.get(index).length);
        }
        if (compressionTypesList != null) {
          System.arraycopy(
              compressionTypesList.get(index),
              0,
              compressionTypes,
              curPos,
              compressionTypesList.get(index).length);
        }
        curPos += measurementsList.get(index).length;
      }

      schemaTree.addDeviceInfo(
          getDeviceSchemaInfoWithAutoCreate(
              entry.getKey(),
              measurements,
              index -> tsDataTypes[index],
              encodings,
              compressionTypes,
              isAligned));
    }

    return schemaTree;
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path) {
    return null;
  }

  @Override
  public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
    return null;
  }

  @Override
  public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
    return null;
  }

  @Override
  public void invalidAllCache() {}
}
