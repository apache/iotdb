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

package org.apache.iotdb.db.tools.validate;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TsFileResourceValidationTool {

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Please input sequence data dir path.");
      return;
    }
    String sequenceDataPath = args[0];
    File sequenceData = new File(sequenceDataPath);
    validateSequenceDataDir(sequenceData);
  }

  public static void validateSequenceDataDir(File sequenceDataDir) throws IOException {
    for (File sg : Objects.requireNonNull(sequenceDataDir.listFiles())) {
      if (!sg.isDirectory()) {
        continue;
      }
      for (File dataRegionDir : Objects.requireNonNull(sg.listFiles())) {
        if (!dataRegionDir.isDirectory()) {
          continue;
        }
        for (File timePartitionDir : Objects.requireNonNull(dataRegionDir.listFiles())) {
          if (!timePartitionDir.isDirectory()) {
            continue;
          }
          checkTimePartitionHasOverlap(timePartitionDir);
        }
      }
    }
  }

  private static void checkTimePartitionHasOverlap(File timePartitionDir) throws IOException {
    List<TsFileResource> resources = loadTsFileResources(timePartitionDir);
    if (resources.isEmpty()) {
      return;
    }
    Map<String, Long> deviceEndTimeMap = new HashMap<>();
    Map<String, TsFileResource> deviceLastExistTsFileMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      Set<String> devices = resource.getDevices();
      for (String device : devices) {
        long deviceStartTimeInCurrentFile = resource.getStartTime(device);
        long deviceEndTimeInCurrentFile = resource.getEndTime(device);
        if (!deviceLastExistTsFileMap.containsKey(device)) {
          deviceEndTimeMap.put(device, deviceEndTimeInCurrentFile);
          deviceLastExistTsFileMap.put(device, resource);
          continue;
        }
        long deviceEndTimeInPreviousFile = deviceEndTimeMap.get(device);
        if (deviceStartTimeInCurrentFile <= deviceEndTimeInPreviousFile) {
          System.out.printf(
              "previous file: %s, current file: %s, device %s in previous file end time is %d,"
                  + " device in current file start time is %d\n",
              deviceLastExistTsFileMap.get(device).getTsFilePath(),
              resource.getTsFilePath(),
              device,
              deviceEndTimeInPreviousFile,
              deviceStartTimeInCurrentFile);
          continue;
        }
        deviceEndTimeMap.put(device, deviceEndTimeInCurrentFile);
        deviceLastExistTsFileMap.put(device, resource);
      }
    }
  }

  public static List<TsFileResource> loadTsFileResources(File timePartitionDir) throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    for (File tsfile : Objects.requireNonNull(timePartitionDir.listFiles())) {
      if (!tsfile.getAbsolutePath().endsWith(TsFileConstant.TSFILE_SUFFIX) || !tsfile.isFile()) {
        continue;
      }
      String resourcePath = tsfile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;
      if (!new File(resourcePath).exists()) {
        continue;
      }
      TsFileResource resource = new TsFileResource(tsfile);
      resource.deserialize();
      resource.buildDeviceTimeIndex();
      resource.close();
      resources.add(resource);
    }

    return resources;
  }
}
