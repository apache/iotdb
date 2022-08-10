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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

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
          schemaTree.appendMeasurementPaths(schemaRegion.getMeasurementPaths(pathPattern, false));
        }
      }
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
    schemaTree.setStorageGroups(new ArrayList<>(storageGroupSet));
    return schemaTree;
  }

  @Override
  public ISchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean aligned) {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();

    PathPatternTree patternTree = new PathPatternTree();
    for (String measurement : measurements) {
      patternTree.appendFullPath(devicePath, measurement);
    }

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    ClusterSchemaTree fetchedSchemaTree;

    if (!config.isAutoCreateSchemaEnabled()) {
      fetchedSchemaTree = fetchSchema(patternTree);
      schemaTree.mergeSchemaTree(fetchedSchemaTree);
      return schemaTree;
    }

    fetchedSchemaTree = fetchSchema(patternTree);
    schemaTree.mergeSchemaTree(fetchedSchemaTree);

    ClusterSchemaTree missingSchemaTree =
        checkAndAutoCreateMissingMeasurements(
            fetchedSchemaTree, devicePath, measurements, tsDataTypes, aligned);

    schemaTree.mergeSchemaTree(missingSchemaTree);

    return schemaTree;
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    PathPatternTree patternTree = new PathPatternTree();
    for (int i = 0; i < devicePathList.size(); i++) {
      for (String measurement : measurementsList.get(i)) {
        patternTree.appendFullPath(devicePathList.get(i), measurement);
      }
    }

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    ClusterSchemaTree fetchedSchemaTree;

    if (!config.isAutoCreateSchemaEnabled()) {
      fetchedSchemaTree = fetchSchema(patternTree);
      schemaTree.mergeSchemaTree(fetchedSchemaTree);
      return schemaTree;
    }

    fetchedSchemaTree = fetchSchema(patternTree);
    schemaTree.mergeSchemaTree(fetchedSchemaTree);

    ClusterSchemaTree missingSchemaTree;
    for (int i = 0; i < devicePathList.size(); i++) {
      missingSchemaTree =
          checkAndAutoCreateMissingMeasurements(
              schemaTree,
              devicePathList.get(i),
              measurementsList.get(i),
              tsDataTypesList.get(i),
              isAlignedList.get(i));
      schemaTree.mergeSchemaTree(missingSchemaTree);
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

  private Pair<List<String>, List<TSDataType>> checkMissingMeasurements(
      ISchemaTree schemaTree,
      PartialPath devicePath,
      String[] measurements,
      TSDataType[] tsDataTypes) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));
    if (deviceSchemaInfo == null) {
      return new Pair<>(Arrays.asList(measurements), Arrays.asList(tsDataTypes));
    }

    List<String> missingMeasurements = new ArrayList<>();
    List<TSDataType> dataTypesOfMissingMeasurement = new ArrayList<>();
    List<MeasurementSchema> schemaList = deviceSchemaInfo.getMeasurementSchemaList();
    for (int i = 0; i < measurements.length; i++) {
      if (schemaList.get(i) == null) {
        missingMeasurements.add(measurements[i]);
        dataTypesOfMissingMeasurement.add(tsDataTypes[i]);
      }
    }

    return new Pair<>(missingMeasurements, dataTypesOfMissingMeasurement);
  }

  private ClusterSchemaTree checkAndAutoCreateMissingMeasurements(
      ISchemaTree schemaTree,
      PartialPath devicePath,
      String[] measurements,
      TSDataType[] tsDataTypes,
      boolean isAligned) {

    Pair<List<String>, List<TSDataType>> checkResult =
        checkMissingMeasurements(schemaTree, devicePath, measurements, tsDataTypes);

    List<String> missingMeasurements = checkResult.left;
    List<TSDataType> dataTypesOfMissingMeasurement = checkResult.right;

    if (missingMeasurements.isEmpty()) {
      return new ClusterSchemaTree();
    }

    internalCreateTimeseries(
        devicePath, missingMeasurements, dataTypesOfMissingMeasurement, isAligned);

    PathPatternTree patternTree = new PathPatternTree();
    for (String measurement : missingMeasurements) {
      patternTree.appendFullPath(devicePath, measurement);
    }
    ClusterSchemaTree reFetchSchemaTree = fetchSchema(patternTree);

    Pair<List<String>, List<TSDataType>> recheckResult =
        checkMissingMeasurements(
            reFetchSchemaTree,
            devicePath,
            missingMeasurements.toArray(new String[0]),
            dataTypesOfMissingMeasurement.toArray(new TSDataType[0]));

    missingMeasurements = recheckResult.left;
    if (!missingMeasurements.isEmpty()) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("(");
      for (String missingMeasurement : missingMeasurements) {
        stringBuilder.append(missingMeasurement).append(" ");
      }
      stringBuilder.append(")");
      throw new RuntimeException(
          String.format(
              "Failed to auto create schema, devicePath: %s, measurements: %s",
              devicePath.getFullPath(), stringBuilder));
    }

    return reFetchSchemaTree;
  }

  private void internalCreateTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> tsDataTypes,
      boolean isAligned) {
    try {
      if (isAligned) {
        CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan = new CreateAlignedTimeSeriesPlan();
        createAlignedTimeSeriesPlan.setPrefixPath(devicePath);
        createAlignedTimeSeriesPlan.setMeasurements(measurements);
        createAlignedTimeSeriesPlan.setDataTypes(tsDataTypes);
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressors = new ArrayList<>();
        for (TSDataType dataType : tsDataTypes) {
          encodings.add(getDefaultEncoding(dataType));
          compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
        }
        createAlignedTimeSeriesPlan.setEncodings(encodings);
        createAlignedTimeSeriesPlan.setCompressors(compressors);
        SchemaRegionId schemaRegionId =
            localConfigNode.getBelongedSchemaRegionIdWithAutoCreate(devicePath);
        ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
        schemaRegion.createAlignedTimeSeries(createAlignedTimeSeriesPlan);
      } else {
        for (int i = 0; i < measurements.size(); i++) {
          CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan();
          createTimeSeriesPlan.setPath(
              new PartialPath(devicePath.getFullPath(), measurements.get(i)));
          createTimeSeriesPlan.setDataType(tsDataTypes.get(i));
          createTimeSeriesPlan.setEncoding(getDefaultEncoding(tsDataTypes.get(i)));
          createTimeSeriesPlan.setCompressor(
              TSFileDescriptor.getInstance().getConfig().getCompressor());
          SchemaRegionId schemaRegionId =
              localConfigNode.getBelongedSchemaRegionIdWithAutoCreate(devicePath);
          ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
          schemaRegion.createTimeseries(createTimeSeriesPlan, -1);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("cannot auto create schema ", e);
    }
  }
}
