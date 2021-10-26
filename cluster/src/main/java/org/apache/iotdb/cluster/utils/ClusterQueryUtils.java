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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.read.common.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusterQueryUtils {

  private static final Logger logger = LoggerFactory.getLogger(ClusterQueryUtils.class);

  private ClusterQueryUtils() {
    // util class
  }

  /**
   * Check if the given path exists locally or can be pulled from a remote node.
   *
   * @param path
   * @throws QueryProcessException
   */
  public static void checkPathExistence(String path) throws QueryProcessException {
    try {
      checkPathExistence(new PartialPath(path));
    } catch (IllegalPathException e) {
      throw new QueryProcessException(e);
    }
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

  public static void checkPathExistence(List<PartialPath> paths) throws QueryProcessException {
    for (PartialPath path : paths) {
      checkPathExistence(path);
    }
  }

  /**
   * Generate path string list for RPC request.
   *
   * <p>If vector path, return its vectorId with all subSensors. Else just return path string.
   */
  public static List<String> getPathStrListForRequest(Path path) {
    if (path instanceof VectorPartialPath) {
      List<String> pathWithSubSensors =
          new ArrayList<>(((VectorPartialPath) path).getSubSensorsList().size() + 1);
      pathWithSubSensors.add(path.getFullPath());
      pathWithSubSensors.addAll(((VectorPartialPath) path).getSubSensorsList());
      return pathWithSubSensors;
    } else {
      return Collections.singletonList(path.getFullPath());
    }
  }

  /**
   * Deserialize an assembled Path from path string list that's from RPC request.
   *
   * <p>This method is corresponding to getPathStringListForRequest().
   */
  public static PartialPath getAssembledPathFromRequest(List<String> pathString) {
    try {
      if (pathString.size() == 1) {
        return new PartialPath(pathString.get(0));
      } else {
        return new VectorPartialPath(pathString.get(0), pathString.subList(1, pathString.size()));
      }
    } catch (IllegalPathException e) {
      logger.error("Failed to create partial path, fullPath is {}.", pathString, e);
      return null;
    }
  }
}
