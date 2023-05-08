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
package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowTimeSeriesResult;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.INodeSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class SchemaRegionTestUtil {

  public static void createTimeseries(
      ISchemaRegion schemaRegion,
      String fullPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias)
      throws MetadataException {
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath(fullPath),
            dataType,
            encoding,
            compressor,
            props,
            tags,
            attributes,
            alias),
        -1);
  }

  public static void createTimeseries(
      ISchemaRegion schemaRegion,
      List<String> fullPaths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> props,
      List<Map<String, String>> tags,
      List<Map<String, String>> attributes,
      List<String> alias)
      throws MetadataException {
    for (int i = 0; i < fullPaths.size(); i++) {
      schemaRegion.createTimeseries(
          SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
              new PartialPath(fullPaths.get(i)),
              dataTypes.get(i),
              encodings.get(i),
              compressors.get(i),
              props == null ? null : props.get(i),
              tags == null ? null : tags.get(i),
              attributes == null ? null : attributes.get(i),
              alias == null ? null : alias.get(i)),
          -1);
    }
  }

  public static void createAlignedTimeseries(
      ISchemaRegion schemaRegion,
      String devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> tags,
      List<Map<String, String>> attributes,
      List<String> alias)
      throws MetadataException {
    schemaRegion.createAlignedTimeSeries(
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath(devicePath),
            measurements,
            dataTypes,
            encodings,
            compressors,
            alias,
            tags,
            attributes));
  }

  /**
   * When testing some interfaces, if you only care about path and do not care the data type or
   * compression type and other details, then use this function to create a timeseries quickly. It
   * returns a CreateTimeSeriesPlanImpl with data type of INT64, TSEncoding of PLAIN, compression
   * type of SNAPPY and without any tags or templates.
   */
  public static void createSimpleTimeSeriesInt64(ISchemaRegion schemaRegion, String path)
      throws Exception {
    SchemaRegionTestUtil.createTimeseries(
        schemaRegion,
        path,
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        null,
        null);
  }

  /**
   * Create timeseries quickly using createSimpleTimeSeriesInt64 with given string list of paths.
   *
   * @param schemaRegion schemaRegion which you want to create timeseries
   * @param pathList
   */
  public static void createSimpleTimeseriesByList(ISchemaRegion schemaRegion, List<String> pathList)
      throws Exception {
    for (String path : pathList) {
      SchemaRegionTestUtil.createSimpleTimeSeriesInt64(schemaRegion, path);
    }
  }

  public static long getAllTimeseriesCount(
      ISchemaRegion schemaRegion,
      PartialPath pathPattern,
      Map<Integer, Template> templateMap,
      boolean isPrefixMatch) {
    try (ISchemaReader<ITimeSeriesSchemaInfo> timeSeriesReader =
        schemaRegion.getTimeSeriesReader(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                pathPattern, templateMap, false, null, null, 0, 0, isPrefixMatch)); ) {
      long count = 0;
      while (timeSeriesReader.hasNext()) {
        timeSeriesReader.next();
        count++;
      }
      return count;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static long getDevicesNum(
      ISchemaRegion schemaRegion, PartialPath pathPattern, boolean isPrefixMatch) {
    try (ISchemaReader<IDeviceSchemaInfo> deviceReader =
        schemaRegion.getDeviceReader(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(pathPattern, isPrefixMatch))) {
      long count = 0;
      while (deviceReader.hasNext()) {
        deviceReader.next();
        count++;
      }
      return count;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      ISchemaRegion schemaRegion, PartialPath pathPattern, int level, boolean isPrefixMatch) {
    try (ISchemaReader<ITimeSeriesSchemaInfo> timeSeriesReader =
        schemaRegion.getTimeSeriesReader(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                pathPattern, null, false, null, null, 0, 0, isPrefixMatch)); ) {
      Map<PartialPath, Long> countMap = new HashMap<>();
      while (timeSeriesReader.hasNext()) {
        ITimeSeriesSchemaInfo timeSeriesSchemaInfo = timeSeriesReader.next();
        PartialPath path = timeSeriesSchemaInfo.getPartialPath();
        if (path.getNodeLength() < level) {
          continue;
        }
        countMap.compute(
            new PartialPath(Arrays.copyOf(path.getNodes(), level + 1)),
            (k, v) -> {
              if (v == null) {
                return 1L;
              }
              return v + 1;
            });
      }
      return countMap;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static List<String> getPathsUsingTemplate(
      ISchemaRegion schemaRegion, PartialPath pathPattern, int templateId) {
    List<String> result = new ArrayList<>();
    try (ISchemaReader<IDeviceSchemaInfo> deviceReader =
        schemaRegion.getDeviceReader(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(
                pathPattern, 0, 0, false, templateId)); ) {
      while (deviceReader.hasNext()) {
        result.add(deviceReader.next().getFullPath());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static List<PartialPath> getNodesListInGivenLevel(
      ISchemaRegion schemaRegion, PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) {
    List<PartialPath> result = new ArrayList<>();
    try (ISchemaReader<INodeSchemaInfo> nodeReader =
        schemaRegion.getNodeReader(
            SchemaRegionReadPlanFactory.getShowNodesPlan(pathPattern, nodeLevel, isPrefixMatch))) {
      while (nodeReader.hasNext()) {
        result.add(nodeReader.next().getPartialPath());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static Set<INodeSchemaInfo> getChildNodePathInNextLevel(
      ISchemaRegion schemaRegion, PartialPath pathPattern) {
    Set<INodeSchemaInfo> result = new HashSet<>();
    try (ISchemaReader<INodeSchemaInfo> nodeReader =
        schemaRegion.getNodeReader(
            SchemaRegionReadPlanFactory.getShowNodesPlan(
                pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD)))) {
      while (nodeReader.hasNext()) {
        result.add(nodeReader.next());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static List<ITimeSeriesSchemaInfo> showTimeseries(
      ISchemaRegion schemaRegion, IShowTimeSeriesPlan plan) {
    List<ITimeSeriesSchemaInfo> result = new ArrayList<>();
    ITimeSeriesSchemaInfo timeSeriesSchemaInfo;
    try (ISchemaReader<ITimeSeriesSchemaInfo> reader = schemaRegion.getTimeSeriesReader(plan)) {
      while (reader.hasNext()) {
        timeSeriesSchemaInfo = reader.next();
        result.add(
            new ShowTimeSeriesResult(
                timeSeriesSchemaInfo.getFullPath(),
                timeSeriesSchemaInfo.getAlias(),
                timeSeriesSchemaInfo.getSchema(),
                timeSeriesSchemaInfo.getTags(),
                timeSeriesSchemaInfo.getAttributes(),
                timeSeriesSchemaInfo.isUnderAlignedDevice()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static List<IDeviceSchemaInfo> getMatchedDevices(
      ISchemaRegion schemaRegion, IShowDevicesPlan plan) {
    List<IDeviceSchemaInfo> result = new ArrayList<>();
    try (ISchemaReader<IDeviceSchemaInfo> reader = schemaRegion.getDeviceReader(plan)) {
      while (reader.hasNext()) {
        result.add(reader.next());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static long deleteTimeSeries(ISchemaRegion schemaRegion, PartialPath pathPattern)
      throws MetadataException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(pathPattern);
    patternTree.constructTree();
    long num = schemaRegion.constructSchemaBlackList(patternTree);
    schemaRegion.deleteTimeseriesInBlackList(patternTree);
    return num;
  }
}
