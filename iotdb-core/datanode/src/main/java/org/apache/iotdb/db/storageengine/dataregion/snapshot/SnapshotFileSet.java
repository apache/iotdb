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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SnapshotFileSet {
  public static final String[] DATA_FILE_SUFFIX =
      new String[] {
        TsFileConstant.TSFILE_SUFFIX.replace(".", ""),
        TsFileResource.RESOURCE_SUFFIX.replace(".", ""),
        ModificationFile.FILE_SUFFIX.replace(".", ""),
      };

  private static final Set<String> DATA_FILE_SUFFIX_SET =
      new HashSet<>(Arrays.asList(DATA_FILE_SUFFIX));

  public static boolean isDataFile(File file) {
    String[] fileName = file.getName().split("\\.");
    return DATA_FILE_SUFFIX_SET.contains(fileName[fileName.length - 1]);
  }
}
