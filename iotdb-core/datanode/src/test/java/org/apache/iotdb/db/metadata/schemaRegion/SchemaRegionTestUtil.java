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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterFactory;
import org.apache.iotdb.commons.schema.filter.impl.DeviceFilterUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.INodeSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowTimeSeriesResult;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

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
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath(fullPath),
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
      schemaRegion.createTimeSeries(
          SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
              new MeasurementPath(fullPaths.get(i)),
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
   * Create time series quickly using createSimpleTimeSeriesInt64 with given string list of paths.
   *
   * @param schemaRegion schemaRegion which you want to create time series
   * @param pathList
   */
  public static void createSimpleTimeSeriesByList(ISchemaRegion schemaRegion, List<String> pathList)
      throws Exception {
    for (String path : pathList) {
      SchemaRegionTestUtil.createSimpleTimeSeriesInt64(schemaRegion, path);
    }
  }

  public static long getAllTimeSeriesCount(
      ISchemaRegion schemaRegion,
      PartialPath pathPattern,
      Map<Integer, Template> templateMap,
      boolean isPrefixMatch) {
    try (ISchemaReader<ITimeSeriesSchemaInfo> timeSeriesReader =
        schemaRegion.getTimeSeriesReader(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                pathPattern, templateMap, 0, 0, isPrefixMatch, null, false, ALL_MATCH_SCOPE))) {
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

  public static void checkSingleTimeSeries(
      final ISchemaRegion schemaRegion,
      final PartialPath pathPattern,
      final boolean isAligned,
      final TSDataType type,
      final TSEncoding encoding,
      final CompressionType compressor,
      final String alias,
      final Map<String, String> tags,
      final Map<String, String> attributes) {
    try (final ISchemaReader<ITimeSeriesSchemaInfo> timeSeriesReader =
        schemaRegion.getTimeSeriesReader(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                pathPattern, Collections.emptyMap(), 0, 0, false, null, false, ALL_MATCH_SCOPE))) {
      Assert.assertTrue(timeSeriesReader.hasNext());
      final ITimeSeriesSchemaInfo info = timeSeriesReader.next();
      Assert.assertEquals(isAligned, info.isUnderAlignedDevice());
      Assert.assertEquals(type, info.getSchema().getType());
      Assert.assertEquals(encoding, info.getSchema().getEncodingType());
      Assert.assertEquals(compressor, info.getSchema().getCompressor());
      Assert.assertEquals(alias, info.getAlias());
      Assert.assertEquals(tags, info.getTags());
      Assert.assertEquals(attributes, info.getAttributes());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static long getDevicesNum(
      ISchemaRegion schemaRegion, PartialPath pathPattern, boolean isPrefixMatch) {
    try (ISchemaReader<IDeviceSchemaInfo> deviceReader =
        schemaRegion.getDeviceReader(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(
                pathPattern, 0, 0, isPrefixMatch, null, ALL_MATCH_SCOPE))) {
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
                pathPattern, null, 0, 0, isPrefixMatch, null, false, ALL_MATCH_SCOPE))) {
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
                pathPattern, 0, 0, false, templateId, ALL_MATCH_SCOPE))) {
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
            SchemaRegionReadPlanFactory.getShowNodesPlan(
                pathPattern, nodeLevel, isPrefixMatch, ALL_MATCH_SCOPE))) {
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
                pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), ALL_MATCH_SCOPE))) {
      while (nodeReader.hasNext()) {
        result.add(nodeReader.next());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static List<ITimeSeriesSchemaInfo> showTimeseries(
      ISchemaRegion schemaRegion, PartialPath path) {
    return showTimeseries(schemaRegion, path, Collections.emptyMap(), 0, 0, false, null, false);
  }

  public static List<ITimeSeriesSchemaInfo> showTimeseries(
      ISchemaRegion schemaRegion, PartialPath path, Map<Integer, Template> relatedTemplate) {
    return showTimeseries(schemaRegion, path, relatedTemplate, 0, 0, false, null, false);
  }

  public static List<ITimeSeriesSchemaInfo> showTimeseries(
      ISchemaRegion schemaRegion,
      PartialPath path,
      boolean isContains,
      String tagKey,
      String tagValue) {
    return showTimeseries(
        schemaRegion,
        path,
        Collections.emptyMap(),
        0,
        0,
        false,
        SchemaFilterFactory.createTagFilter(tagKey, tagValue, isContains),
        false);
  }

  public static List<ITimeSeriesSchemaInfo> showTimeseries(
      ISchemaRegion schemaRegion,
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      long limit,
      long offset,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      boolean needViewDetail) {
    List<ITimeSeriesSchemaInfo> result = new ArrayList<>();
    ITimeSeriesSchemaInfo timeSeriesSchemaInfo;
    try (ISchemaReader<ITimeSeriesSchemaInfo> reader =
        schemaRegion.getTimeSeriesReader(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                path,
                relatedTemplate,
                limit,
                offset,
                isPrefixMatch,
                schemaFilter,
                needViewDetail,
                ALL_MATCH_SCOPE))) {
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
      ISchemaRegion schemaRegion, PartialPath path) {
    return getMatchedDevices(schemaRegion, path, 0, 0, false);
  }

  public static List<IDeviceSchemaInfo> getMatchedDevices(
      ISchemaRegion schemaRegion, PartialPath path, int limit, int offset, boolean isPrefixMatch) {
    return getMatchedDevices(schemaRegion, path, limit, offset, isPrefixMatch, null);
  }

  public static List<IDeviceSchemaInfo> getMatchedDevices(
      ISchemaRegion schemaRegion,
      PartialPath path,
      int limit,
      int offset,
      boolean isPrefixMatch,
      SchemaFilter filter) {
    List<IDeviceSchemaInfo> result = new ArrayList<>();
    try (ISchemaReader<IDeviceSchemaInfo> reader =
        schemaRegion.getDeviceReader(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(
                path, limit, offset, isPrefixMatch, filter, ALL_MATCH_SCOPE))) {
      while (reader.hasNext()) {
        result.add(reader.next());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static Pair<Long, Boolean> deleteTimeSeries(
      final ISchemaRegion schemaRegion, final PartialPath pathPattern) throws MetadataException {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(pathPattern);
    patternTree.constructTree();
    final Pair<Long, Boolean> numIsViewPair = schemaRegion.constructSchemaBlackList(patternTree);
    schemaRegion.deleteTimeseriesInBlackList(patternTree);
    return numIsViewPair;
  }

  public static void createTableDevice(
      final ISchemaRegion schemaRegion,
      final String table,
      final Object[] deviceIds,
      final Map<String, String> attributes)
      throws MetadataException {
    schemaRegion.createOrUpdateTableDevice(
        new CreateOrUpdateTableDeviceNode(
            new PlanNodeId(""),
            null,
            table,
            Collections.singletonList(deviceIds),
            new ArrayList<>(attributes.keySet()),
            Collections.singletonList(
                attributes.values().stream()
                    .map(s -> s != null ? new Binary(s.getBytes(StandardCharsets.UTF_8)) : null)
                    .toArray())));
  }

  public static List<IDeviceSchemaInfo> getTableDevice(
      final ISchemaRegion schemaRegion, final String table, final List<String[]> deviceIdList) {
    final List<IDeviceSchemaInfo> result = new ArrayList<>();
    try (final ISchemaReader<IDeviceSchemaInfo> reader =
        schemaRegion.getTableDeviceReader(table, new ArrayList<>(deviceIdList))) {
      while (reader.hasNext()) {
        result.add(reader.next());
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static List<IDeviceSchemaInfo> getTableDevice(
      final ISchemaRegion schemaRegion,
      final String table,
      final int idColumnNum,
      final List<SchemaFilter> idDeterminedFilterList) {
    final List<PartialPath> patternList =
        DeviceFilterUtil.convertToDevicePattern(
            schemaRegion.getDatabaseFullPath().substring(ROOT.length() + 1),
            table,
            idColumnNum,
            Collections.singletonList(idDeterminedFilterList));
    final List<IDeviceSchemaInfo> result = new ArrayList<>();
    for (final PartialPath pattern : patternList) {
      try (final ISchemaReader<IDeviceSchemaInfo> reader =
          schemaRegion.getTableDeviceReader(pattern)) {
        while (reader.hasNext()) {
          result.add(reader.next());
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }
}
