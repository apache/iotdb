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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class FilePathUtils {

  private static final String PATH_SPLIT_STRING = File.separator.equals("\\") ? "\\\\" : "/";

  private FilePathUtils() {
    // forbidding instantiation
  }

  /**
   * Format file path to end with File.separator
   * @param filePath origin file path
   * @return Regularized Path
   */
  public static String regularizePath(String filePath){
    if (filePath.length() > 0
        && filePath.charAt(filePath.length() - 1) != File.separatorChar) {
      filePath = filePath + File.separatorChar;
    }
    return filePath;
  }

  public static String[] splitTsFilePath(TsFileResource resource) {
    return resource.getTsFile().getAbsolutePath().split(PATH_SPLIT_STRING);
  }

  /**
   * get paths from group by level, like root.sg1.d2.s0, root.sg1.d1.s1
   * level=1, return [root.sg1, 0] and pathIndex turns to be [[0, root.sg1], [1, root.sg1]]
   * @param rawPaths
   * @param level
   * @param pathIndex
   * @return
   */
  public static Map<String, Long> getPathByLevel(List<PartialPath> rawPaths, int level, Map<Integer, String> pathIndex)
      throws QueryProcessException {
    // pathGroupByLevel -> count
    Map<String, Long> finalPaths = new TreeMap<>();

    int i = 0;
    for (PartialPath value : rawPaths) {
      String[] tmpPath;
      try {
        tmpPath = MetaUtils.splitPathToDetachedPath(value.getFullPath());
      } catch (IllegalPathException e) {
        throw new QueryProcessException(e.getMessage());
      }

      String key;
      if (tmpPath.length <= level) {
        key = value.getFullPath();
      } else {
        StringBuilder path = new StringBuilder();
        for (int k = 0; k <= level; k++) {
          if (k == 0) {
            path.append(tmpPath[k]);
          } else {
            path.append(TsFileConstant.PATH_SEPARATOR).append(tmpPath[k]);
          }
        }
        key = path.toString();
      }
      finalPaths.putIfAbsent(key, 0L);
      if (pathIndex != null) {
        pathIndex.put(i++, key);
      }
    }

    return finalPaths;
  }

  /**
   * merge the raw record by level, for example
   * raw record [timestamp, root.sg1.d1.s0, root.sg1.d1.s1, root.sg1.d2.s2], level=1
   * and newRecord data is [100, 1, 1, 1]
   * return [100, 3]
   *
   * @param newRecord
   * @param finalPaths
   * @param pathIndex
   * @return
   */
  public static RowRecord mergeRecordByPath(RowRecord newRecord,
                                      Map<String, Long> finalPaths,
                                      Map<Integer, String> pathIndex) {
    if (newRecord.getFields().size() < finalPaths.size()) {
      return null;
    }

    // reset final paths
    for (Map.Entry<String, Long> entry : finalPaths.entrySet()) {
      entry.setValue(0L);
    }

    RowRecord tmpRecord = new RowRecord(newRecord.getTimestamp());

    for (int i = 0; i < newRecord.getFields().size(); i++) {
      if (newRecord.getFields().get(i) != null) {
        finalPaths.put(pathIndex.get(i),
          finalPaths.get(pathIndex.get(i)) + newRecord.getFields().get(i).getLongV());
      }
    }

    for (Map.Entry<String, Long> entry : finalPaths.entrySet()) {
      tmpRecord.addField(Field.getField(entry.getValue(), TSDataType.INT64));
    }

    return tmpRecord;
  }

}
