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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class ClusterQueryUtils {

  private static final Logger logger = LoggerFactory.getLogger(ClusterQueryUtils.class);

  private ClusterQueryUtils() {
    // util class
  }

  public static void checkPathExistence(PartialPath path) throws QueryProcessException {
    if (!IoTDB.metaManager.isPathExist(path)) {
      try {
        MetaPuller.getInstance().pullTimeSeriesSchemas(Collections.singletonList(path), null);
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }
  }

  /**
   * Generate path string list for RPC request.
   *
   * <p>If vector path, return its vectorId with all subSensors. Else just return path string. TODO
   * aligned path
   */
  public static String getPathStrListForRequest(Path path) {
    // TODO aligned Path
    return path.getFullPath();
  }

  /**
   * Deserialize an assembled Path from path string list that's from RPC request.
   *
   * <p>This method is corresponding to getPathStringListForRequest().
   */
  public static MeasurementPath getAssembledPathFromRequest(String pathString, byte dataType) {
    // TODO aligned path
    try {
      MeasurementPath matchedPath = new MeasurementPath(pathString);
      matchedPath.setMeasurementSchema(
          new UnaryMeasurementSchema(
              matchedPath.getMeasurement(), TSDataType.deserialize(dataType)));
      return matchedPath;
    } catch (IllegalPathException e) {
      logger.error("Failed to create partial path, fullPath is {}.", pathString, e);
      return null;
    }
  }
}
