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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.fileSystem.FSFactoryProducer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

public class IoTDBDataDirViewer {

  public static void main(String[] args) throws IOException {
    String[] data_dir;
    String outFile = "IoTDB_data_dir_overview.txt";
    if (args.length == 0) {
      String path = "data/datanode/data";
      data_dir = path.split(","); // multiple data dirs separated by comma
    } else if (args.length == 1) {
      data_dir = args[0].split(",");
    } else if (args.length == 2) {
      data_dir = args[0].split(",");
      outFile = args[1];
    } else {
      throw new IOException("Invalid parameters. Please check the user guide.");
    }
    System.out.println("output save path:" + outFile);
    System.out.println("data dir num:" + data_dir.length);
    try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
      for (String dir : data_dir) {
        File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);
        File[] seqAndUnseqDirs = dirFile.listFiles();
        if (seqAndUnseqDirs == null) {
          throw new IOException(
              "Irregular data dir structure.There should be a sequence and unsequence directory "
                  + "under the data directory "
                  + dirFile.getName());
        }
        List<File> fileList = Arrays.asList(seqAndUnseqDirs);
        fileList.sort((Comparator.comparing(File::getName)));
        if (!"sequence".equals(seqAndUnseqDirs[1].getName())
            || !"unsequence".equals(seqAndUnseqDirs[2].getName())) {
          throw new IOException(
              "Irregular data dir structure.There should be a sequence and unsequence directory "
                  + "under the data directory "
                  + dirFile.getPath());
        }

        printlnBoth(pw, "|==============================================================");
        printlnBoth(pw, "|" + dir);
        printlnBoth(pw, "|--sequence");
        printFilesInSeqOrUnseqDir(seqAndUnseqDirs[1], pw);
        printlnBoth(pw, "|--unsequence");
        printFilesInSeqOrUnseqDir(seqAndUnseqDirs[2], pw);
      }
      printlnBoth(pw, "|==============================================================");
    }
  }

  private static void printFilesInSeqOrUnseqDir(File seqOrUnseqDir, PrintWriter pw)
      throws IOException {
    File[] storageGroupDirs = seqOrUnseqDir.listFiles();
    if (storageGroupDirs == null) {
      throw new IOException(
          "Irregular data dir structure.There should be database directories under "
              + "the sequence/unsequence directory "
              + seqOrUnseqDir.getName());
    }
    List<File> fileList = Arrays.asList(storageGroupDirs);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File storageGroup : storageGroupDirs) {
      printlnBoth(pw, "|  |--" + storageGroup.getName());
      printFilesInStorageGroupDir(storageGroup, pw);
    }
  }

  private static void printFilesInStorageGroupDir(File storageGroup, PrintWriter pw)
      throws IOException {
    File[] files = storageGroup.listFiles();
    if (files == null) {
      throw new IOException(
          "Irregular data dir structure.There should be dataRegion directories under "
              + "the database directory "
              + storageGroup.getName());
    }
    List<File> fileList = Arrays.asList(files);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File file : fileList) {
      printlnBoth(pw, "|  |  |--" + file.getName());
      printFilesInDataRegionDir(file, pw);
    }
  }

  private static void printFilesInDataRegionDir(File dataRegion, PrintWriter pw)
      throws IOException {
    File[] files = dataRegion.listFiles();
    if (files == null) {
      throw new IOException(
          "Irregular data dir structure.There should be timeInterval directories under "
              + "the database directory "
              + dataRegion.getName());
    }
    List<File> fileList = Arrays.asList(files);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File file : fileList) {
      printlnBoth(pw, "|  |  |  |--" + file.getName());
      printFilesInTimeInterval(file, pw);
    }
  }

  private static void printFilesInTimeInterval(File timeInterval, PrintWriter pw)
      throws IOException {
    File[] files = timeInterval.listFiles();
    if (files == null) {
      throw new IOException(
          "Irregular data dir structure.There should be tsfiles under "
              + "the timeInterval directories directory "
              + timeInterval.getName());
    }
    List<File> fileList = Arrays.asList(files);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File file : fileList) {
      printlnBoth(pw, "|  |  |  |  |--" + file.getName());

      // To print the content if it is a tsfile.resource
      if (file.getName().endsWith(".tsfile.resource")) {
        printResource(file.getAbsolutePath(), pw);
      }
    }
  }

  private static void printResource(String filename, PrintWriter pw) throws IOException {
    filename = filename.substring(0, filename.length() - 9);
    TsFileResource resource = new TsFileResource(SystemFileFactory.INSTANCE.getFile(filename));
    resource.deserialize();
    // sort device strings
    SortedSet<IDeviceID> keys = new TreeSet<>(resource.getDevices());
    for (IDeviceID device : keys) {
      // iterating the index, must present
      //noinspection OptionalGetWithoutIsPresent
      Optional<Long> startTime = resource.getStartTime(device);
      long start;
      if (!startTime.isPresent()) {
        printlnBoth(pw, String.format("|  |  |  |  |  |--device %s, start time is null", device));
        return;
      } else {
        start = startTime.get();
      }
      long end;
      Optional<Long> endTime = resource.getStartTime(device);
      if (!endTime.isPresent()) {
        printlnBoth(pw, String.format("|  |  |  |  |  |--device %s, end time is null", device));
        return;
      } else {
        end = endTime.get();
      }
      printlnBoth(
          pw,
          String.format(
              "|  |  |  |  |  |--device %s, start time %d (%s), end time %d (%s)",
              device,
              start,
              DateTimeUtils.convertLongToDate(start),
              end,
              DateTimeUtils.convertLongToDate(end)));
    }
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }
}
