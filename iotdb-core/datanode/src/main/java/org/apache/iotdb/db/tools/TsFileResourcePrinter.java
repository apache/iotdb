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

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.fileSystem.FSFactoryProducer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * This tool can analyze the tsfile.resource files from a folder, or analyze a single
 * tsfile.resource file.
 */
public class TsFileResourcePrinter {

  @SuppressWarnings("squid:S106")
  public static void main(String[] args) throws IOException {

    String folder = "data/data/sequence/root.group_1/0";
    if (args.length >= 1) {
      folder = args[0];
    }
    File folderFile = SystemFileFactory.INSTANCE.getFile(folder);
    if (folderFile.isDirectory()) {
      // analyze the tsfile.resource files from a folder
      File[] files =
          FSFactoryProducer.getFSFactory()
              .listFilesBySuffix(folderFile.getAbsolutePath(), ".tsfile.resource");
      Arrays.sort(files, Comparator.comparingLong(x -> Long.parseLong(x.getName().split("-")[0])));

      for (File file : files) {
        printResource(file.getAbsolutePath());
      }
      System.out.println(
          String.format(
              DataNodeMiscMessages.LOG_ANALYZING_THE_RESOURCE_FILE_FOLDER_ARG_FINISHED_75334FCE,
              Objects.toString(folder)));
    } else {
      // analyze a tsfile.resource file
      printResource(folderFile.getAbsolutePath());
      System.out.println(
          String.format(
              DataNodeMiscMessages.LOG_ANALYZING_THE_RESOURCE_FILE_ARG_FINISHED_95367812,
              Objects.toString(folder)));
    }
  }

  @SuppressWarnings("squid:S106")
  public static void printResource(String filename) throws IOException {
    filename = filename.substring(0, filename.length() - 9);
    TsFileResource resource = new TsFileResource();
    resource.setFile(SystemFileFactory.INSTANCE.getFile(filename));
    System.out.printf(DataNodeMiscMessages.LOG_ANALYZING_ARG_ARG_0AFCE795, filename);
    System.out.println();
    resource.deserialize();
    System.out.println(
        String.format(
            DataNodeMiscMessages.LOG_TIME_INDEX_TYPE_ARG_63B81EC5,
            Objects.toString(resource.getTimeIndexType())));

    System.out.printf(
        DataNodeMiscMessages.LOG_RESOURCE_PLAN_INDEX_RANGE_ARG_ARG_ARG_BB9E5A74,
        resource.getMinPlanIndex(),
        resource.getMaxPlanIndex());

    for (IDeviceID device : resource.getDevices()) {
      // iterating the index, must present
      //noinspection OptionalGetWithoutIsPresent
      System.out.printf(
          DataNodeMiscMessages.LOG_DEVICE_ARG_START_TIME_ARG_ARG_END_TIME_ARG_ARG_ARG_7CC98253,
          device,
          resource.getStartTime(device).get(),
          DateTimeUtils.convertLongToDate(resource.getStartTime(device).get()),
          resource.getEndTime(device).get(),
          DateTimeUtils.convertLongToDate(resource.getEndTime(device).get()));
    }
    System.out.println();
  }
}
