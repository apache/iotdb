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

package org.apache.iotdb.db.metadata.tagSchemaRegion.utils;

import java.util.Map;
import java.util.TreeMap;

public class PathTagConverterUtils {

  public static Map<String, String> pathToTags(String storageGroupFullPath, String path) {
    if (path.length() <= storageGroupFullPath.length()) return new TreeMap<>();
    String devicePath = path.substring(storageGroupFullPath.length() + 1);
    String[] tags = devicePath.split("\\.");
    Map<String, String> tagsMap = new TreeMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      tagsMap.put(tags[i], tags[i + 1]);
    }
    return tagsMap;
  }

  public static String tagsToPath(String storageGroupFullPath, Map<String, String> tags) {
    StringBuilder stringBuilder = new StringBuilder(storageGroupFullPath);
    for (String tagKey : tags.keySet()) {
      stringBuilder.append(".").append(tagKey).append(".").append(tags.get(tagKey));
    }
    return stringBuilder.toString();
  }

  public static String pathToTagsSortPath(String storageGroupFullPath, String path) {
    return tagsToPath(storageGroupFullPath, pathToTags(storageGroupFullPath, path));
  }
}
