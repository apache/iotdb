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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

/**
 * this tool can analyze the tsfile.resource files from a folder.
 */
public class TsFileResourcePrinter {

  public static void main(String[] args) throws IOException {

    String folder = "test";
    if (args.length >= 1) {
      folder = args[0];
    }
    File folderFile = SystemFileFactory.INSTANCE.getFile(folder);
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix(folderFile.getAbsolutePath(), ".tsfile.resource");
    Arrays.sort(files, Comparator.comparingLong(x -> Long.valueOf(x.getName().split("-")[0])));

    for (File file : files) {
      printResource(file.getAbsolutePath());
    }
    System.out.println("analyzing the resource file finished.");
  }

  public static void printResource(String filename) throws IOException {
    filename = filename.substring(0, filename.length() - 9);
    TsFileResource resource = new TsFileResource(SystemFileFactory.INSTANCE.getFile(filename));
    System.err.println(String.format("analyzing %s ...", filename));
    resource.deSerialize();

    System.out.println("historicalVersions: " + resource.getHistoricalVersions());

    for (String device : resource.getStartTimeMap().keySet()) {
      System.out.println(String.format(
          "device %s, "
              + "start time %d (%s), "
              + "end time %d (%s)",
          device,
          resource.getStartTimeMap().get(device),
          DatetimeUtils.convertMillsecondToZonedDateTime(resource.getStartTimeMap().get(device)),
          resource.getEndTimeMap().get(device),
          DatetimeUtils.convertMillsecondToZonedDateTime(resource.getEndTimeMap().get(device))));
    }
  }
}
