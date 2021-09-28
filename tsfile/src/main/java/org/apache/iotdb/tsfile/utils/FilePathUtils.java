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

package org.apache.iotdb.tsfile.utils;

import java.io.File;

public class FilePathUtils {
  public static String PATH_SPLIT_STRING = File.separator.equals("\\") ? "\\\\" : "/";

  public static String getPartialFilePathForCache(String filePath) {
    if (filePath == null) return null;
    String[] pathSegments = filePath.split(PATH_SPLIT_STRING);
    return pathSegments[pathSegments.length - 4] // storage group name
        + PATH_SPLIT_STRING
        + pathSegments[pathSegments.length - 3] // virtual storage group Id
        + PATH_SPLIT_STRING
        + pathSegments[pathSegments.length - 2] // time partition Id
    ;
  }
}
