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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.HashSet;
import java.util.Set;

public class AggregateUtils {

  /**
   * Transform an originalPath to a partial path that satisfies given level. Path nodes exceed the
   * given level will be replaced by "*", e.g. generatePartialPathByLevel("root.sg.dh.d1.s1", 2)
   * will return "root.sg.dh.*.s1"
   *
   * @param originalPath the original timeseries path
   * @return result partial path
   */
  public static String generatePartialPathByLevel(String originalPath, int[] pathLevels)
      throws IllegalPathException {
    String[] tmpPath = MetaUtils.splitPathToDetachedPath(originalPath);
    Set<Integer> levelSet = new HashSet<>();
    for (int level : pathLevels) {
      levelSet.add(level);
    }

    StringBuilder transformedPath = new StringBuilder();
    transformedPath.append(tmpPath[0]);
    for (int k = 1; k < tmpPath.length - 1; k++) {
      transformedPath.append(TsFileConstant.PATH_SEPARATOR);
      if (levelSet.contains(k)) {
        transformedPath.append(tmpPath[k]);
      } else {
        transformedPath.append(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD);
      }
    }
    transformedPath.append(TsFileConstant.PATH_SEPARATOR).append(tmpPath[tmpPath.length - 1]);
    return transformedPath.toString();
  }
}
