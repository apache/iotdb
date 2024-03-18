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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * This tool can analyze the tsfile.resource files from a folder, or analyze a single
 * tsfile.resource file.
 */
public class ModsGenerator {
  static Set<String> devices = new HashSet<>();
  static long endTime = 1704038400000L + 16200; // delete data before 2023/2/5 00:00:00

  static String filePath =
      "/Users/bensonchou/Desktop/Experients/Experiment3/DirtyData/new/0-0-0-0.tsfile";

  static String targetDir = "/Users/bensonchou/Desktop/Experients/Experiment3/DirtyData/new/";

  String COMPACTION_TEST_SG = "root.test.g_0";

  @SuppressWarnings("squid:S106")
  public static void main(String[] args) throws IOException {
    TsFileResource resource = new TsFileResource(new File(filePath));
    resource.deserialize();
    generateMods(resource);
    System.out.println("Device num is " + resource.getDevices().size());
  }

  public static void generateMods(TsFileResource resource) {
    String filePath = targetDir + resource.getTsFile().getName() + ModificationFile.FILE_SUFFIX;
    try (BufferedOutputStream bufferedOutputStream =
        new BufferedOutputStream(new FileOutputStream(filePath))) {
      StringBuilder stringBuilder = new StringBuilder("");

      for (int i = 0; i < 1; i++) {
        for (String device : resource.getDevices()) {

          stringBuilder.append(
              "DELETION,"
                  + device
                  + ".**,"
                  + Long.MAX_VALUE
                  + ","
                  + Long.MIN_VALUE
                  + ","
                  + endTime
                  + "\n");
        }
      }
      byte[] bytes = stringBuilder.toString().getBytes();
      bufferedOutputStream.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
