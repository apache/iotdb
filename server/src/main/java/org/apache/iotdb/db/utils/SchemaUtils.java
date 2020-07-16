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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SchemaUtils {

  private SchemaUtils() {

  }

  private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);

  public static void registerTimeseries(TimeseriesSchema schema) {
    try {
      logger.debug("Registering timeseries {}", schema);
      String path = schema.getFullPath();
      TSDataType dataType = schema.getType();
      TSEncoding encoding = schema.getEncodingType();
      CompressionType compressionType = schema.getCompressor();
      IoTDB.metaManager.createTimeseries(path, dataType, encoding,
          compressionType, Collections.emptyMap());
    } catch (PathAlreadyExistException ignored) {
      // ignore added timeseries
    } catch (MetadataException e) {
      logger.error("Cannot create timeseries {} in snapshot, ignored", schema.getFullPath(),
          e);
    }

  }

  public static List<TSDataType> getSeriesTypesByPath(Collection<Path> paths)
      throws MetadataException {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : paths) {
      dataTypes.add(IoTDB.metaManager.getSeriesType(path.getFullPath()));
    }
    return dataTypes;
  }

  /**
   * @param paths time series paths
   * @param aggregation aggregation function, may be null
   * @return The data type of aggregation or (data type of paths if aggregation is null)
   */
  public static List<TSDataType> getSeriesTypesByString(Collection<String> paths,
      String aggregation) throws MetadataException {
    TSDataType dataType = getAggregationType(aggregation);
    if (dataType != null) {
      return Collections.nCopies(paths.size(), dataType);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    for (String path : paths) {
      dataTypes.add(IoTDB.metaManager.getSeriesType(path));
    }
    return dataTypes;
  }

  public static List<TSDataType> getSeriesTypesByPath(Collection<Path> paths,
      String aggregation) throws MetadataException {
    TSDataType dataType = getAggregationType(aggregation);
    if (dataType != null) {
      return Collections.nCopies(paths.size(), dataType);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : paths) {
      dataTypes.add(IoTDB.metaManager.getSeriesType(path.getFullPath()));
    }
    return dataTypes;
  }

  public static List<TSDataType> getSeriesTypesByPath(List<Path> paths,
      List<String> aggregations) throws MetadataException {
    List<TSDataType> tsDataTypes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      TSDataType dataType = getAggregationType(aggregations.get(i));
      if (dataType != null) {
        tsDataTypes.add(dataType);
      } else {
        tsDataTypes.add(IoTDB.metaManager.getSeriesType(paths.get(i).getFullPath()));
      }
    }
    return tsDataTypes;
  }

  /**
   * @param aggregation aggregation function
   * @return the data type of the aggregation or null if it aggregation is null
   */
  public static TSDataType getAggregationType(String aggregation) throws MetadataException {
    if (aggregation == null) {
      return null;
    }
    switch (aggregation.toLowerCase()) {
      case SQLConstant.MIN_TIME:
      case SQLConstant.MAX_TIME:
      case SQLConstant.COUNT:
        return TSDataType.INT64;
      case SQLConstant.LAST_VALUE:
      case SQLConstant.FIRST_VALUE:
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
        return null;
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return TSDataType.DOUBLE;
      default:
        throw new MetadataException(
            "aggregate does not support " + aggregation + " function.");
    }
  }
}
