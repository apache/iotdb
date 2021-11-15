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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaUtils {

  private SchemaUtils() {}

  private static final Map<TSDataType, Set<TSEncoding>> schemaChecker =
      new EnumMap<>(TSDataType.class);

  static {
    Set<TSEncoding> booleanSet = new HashSet<>();
    booleanSet.add(TSEncoding.PLAIN);
    booleanSet.add(TSEncoding.RLE);
    schemaChecker.put(TSDataType.BOOLEAN, booleanSet);

    Set<TSEncoding> intSet = new HashSet<>();
    intSet.add(TSEncoding.PLAIN);
    intSet.add(TSEncoding.RLE);
    intSet.add(TSEncoding.TS_2DIFF);
    intSet.add(TSEncoding.GORILLA);
    schemaChecker.put(TSDataType.INT32, intSet);
    schemaChecker.put(TSDataType.INT64, intSet);

    Set<TSEncoding> floatSet = new HashSet<>();
    floatSet.add(TSEncoding.PLAIN);
    floatSet.add(TSEncoding.RLE);
    floatSet.add(TSEncoding.TS_2DIFF);
    floatSet.add(TSEncoding.GORILLA_V1);
    floatSet.add(TSEncoding.GORILLA);
    schemaChecker.put(TSDataType.FLOAT, floatSet);
    schemaChecker.put(TSDataType.DOUBLE, floatSet);

    Set<TSEncoding> textSet = new HashSet<>();
    textSet.add(TSEncoding.PLAIN);
    textSet.add(TSEncoding.DICTIONARY);
    schemaChecker.put(TSDataType.TEXT, textSet);
  }

  private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);

  public static void registerTimeseries(TimeseriesSchema schema) {
    try {
      logger.debug("Registering timeseries {}", schema);
      PartialPath path = new PartialPath(schema.getFullPath());
      TSDataType dataType = schema.getType();
      TSEncoding encoding = schema.getEncodingType();
      CompressionType compressionType = schema.getCompressor();
      IoTDB.metaManager.createTimeseries(
          path, dataType, encoding, compressionType, Collections.emptyMap());
    } catch (PathAlreadyExistException ignored) {
      // ignore added timeseries
    } catch (MetadataException e) {
      if (!(e.getCause() instanceof ClosedByInterruptException)
          && !(e.getCause() instanceof ClosedChannelException)) {
        logger.error("Cannot create timeseries {} in snapshot, ignored", schema.getFullPath(), e);
      }
    }
  }

  public static void cacheTimeseriesSchema(TimeseriesSchema schema) {
    PartialPath path;
    try {
      path = new PartialPath(schema.getFullPath());
    } catch (IllegalPathException e) {
      logger.error("Cannot cache an illegal path {}", schema.getFullPath());
      return;
    }
    TSDataType dataType = schema.getType();
    TSEncoding encoding = schema.getEncodingType();
    CompressionType compressionType = schema.getCompressor();
    IMeasurementSchema measurementSchema =
        new UnaryMeasurementSchema(path.getMeasurement(), dataType, encoding, compressionType);

    IMeasurementMNode measurementMNode =
        MeasurementMNode.getMeasurementMNode(null, path.getMeasurement(), measurementSchema, null);
    IoTDB.metaManager.cacheMeta(path, measurementMNode, true);
  }

  public static List<TSDataType> getSeriesTypesByPaths(Collection<PartialPath> paths)
      throws MetadataException {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (PartialPath path : paths) {
      dataTypes.add(path == null ? null : IoTDB.metaManager.getSeriesType(path));
    }
    return dataTypes;
  }

  /**
   * If the datatype of 'aggregation' depends on 'measurementDataType' (min_value, max_value),
   * return 'measurementDataType' directly, or return a list whose elements are all the datatype of
   * 'aggregation' and its length is the same as 'measurementDataType'.
   *
   * @param measurementDataType
   * @param aggregation
   * @return
   */
  public static List<TSDataType> getAggregatedDataTypes(
      List<TSDataType> measurementDataType, String aggregation) {
    TSDataType dataType = getAggregationType(aggregation);
    if (dataType != null) {
      return Collections.nCopies(measurementDataType.size(), dataType);
    }
    return measurementDataType;
  }

  public static TSDataType getSeriesTypeByPath(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getSeriesType(path);
  }

  public static List<TSDataType> getSeriesTypesByPaths(
      List<PartialPath> paths, List<String> aggregations) throws MetadataException {
    List<TSDataType> tsDataTypes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      String aggrStr = aggregations != null ? aggregations.get(i) : null;
      TSDataType dataType = getAggregationType(aggrStr);
      if (dataType != null) {
        tsDataTypes.add(dataType);
      } else {
        PartialPath path = paths.get(i);
        tsDataTypes.add(path == null ? null : IoTDB.metaManager.getSeriesType(path));
      }
    }
    return tsDataTypes;
  }

  /**
   * @param aggregation aggregation function
   * @return the data type of the aggregation or null if it aggregation is null
   */
  public static TSDataType getAggregationType(String aggregation) {
    if (aggregation == null) {
      return null;
    }
    switch (aggregation.toLowerCase()) {
      case SQLConstant.MIN_TIME:
      case SQLConstant.MAX_TIME:
      case SQLConstant.COUNT:
        return TSDataType.INT64;
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return TSDataType.DOUBLE;
      case SQLConstant.LAST_VALUE:
      case SQLConstant.FIRST_VALUE:
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
      default:
        return null;
    }
  }

  /**
   * If e or one of its recursive causes is a PathNotExistException or StorageGroupNotSetException,
   * return such an exception or null if it cannot be found.
   *
   * @param currEx
   * @return null or a PathNotExistException or a StorageGroupNotSetException
   */
  public static Throwable findMetaMissingException(Throwable currEx) {
    while (true) {
      if (currEx instanceof PathNotExistException
          || currEx instanceof StorageGroupNotSetException) {
        return currEx;
      }
      if (currEx.getCause() == null) {
        break;
      }
      currEx = currEx.getCause();
    }
    return null;
  }

  public static void checkDataTypeWithEncoding(TSDataType dataType, TSEncoding encoding)
      throws MetadataException {
    if (!schemaChecker.get(dataType).contains(encoding)) {
      throw new MetadataException(
          String.format("encoding %s does not support %s", encoding, dataType), true);
    }
  }
}
