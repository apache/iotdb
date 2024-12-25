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

import org.apache.iotdb.db.tools.utils.TsFileValidationScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * This tool can be used to check the correctness of tsfile and point out errors in specific
 * timeseries or devices. The types of errors include the following:
 *
 * <p>Device overlap between files
 *
 * <p>Timeseries overlap between files
 *
 * <p>Timeseries overlap between chunks
 *
 * <p>Timeseries overlap between pages
 *
 * <p>Timeseries overlap within one page
 */
public class TsFileValidationTool {
  // print detail type of overlap or not
  private static boolean printDetails = false;

  // print to local file or not
  private static boolean printToFile = false;
  private static boolean ignoreFileOverlap = false;

  private static String outFilePath = "TsFile_validation_view.txt";

  private static PrintWriter pw = null;

  private static final Logger logger = LoggerFactory.getLogger(TsFileValidationTool.class);
  private static final List<File> seqDataDirList = new ArrayList<>();
  private static final List<File> fileList = new ArrayList<>();

  private static TsFileValidationScan validationScan = new TsFileValidationScan();

  /**
   * The form of param is: [path of data dir or tsfile] [-pd = print details or not] [-f = path of
   * outFile]. Eg: xxx/iotdb/data/data1 xxx/xxx.tsfile -pd=true -f=xxx/TsFile_validation_view.txt
   *
   * <p>The first parameter is required, the others are optional.
   */
  public static void main(String[] args) throws IOException {
    if (!checkArgs(args)) {
      System.exit(1);
    }
    if (printToFile) {
      pw = new PrintWriter(new FileWriter(outFilePath));
      validationScan.setPrintWriter(pw);
    }
    if (printDetails) {
      printBoth("Start checking seq files ...");
      validationScan.setPrintDetails(printDetails);
    }
    validationScan.setIgnoreFileOverlap(ignoreFileOverlap);

    // check tsfile
    for (File f : fileList) {
      findIncorrectFiles(Collections.singletonList(f));
    }

    // check tsfiles in data dir, which will check for correctness inside one single tsfile and
    // between files
    for (File seqDataDir : seqDataDirList) {
      // get sg data dirs
      if (!checkIsDirectory(seqDataDir)) {
        continue;
      }
      List<File> rootTsFiles =
          Arrays.asList(
              Objects.requireNonNull(
                  seqDataDir.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX))));
      findIncorrectFiles(rootTsFiles);

      List<File> sgDirs =
          Arrays.asList(Objects.requireNonNull(seqDataDir.listFiles(File::isDirectory)));
      for (File sgDir : Objects.requireNonNull(sgDirs)) {
        if (!checkIsDirectory(sgDir)) {
          continue;
        }
        if (printDetails) {
          printBoth("- Check files in database: " + sgDir.getAbsolutePath());
        }
        // get data region dirs
        File[] dataRegionDirs = sgDir.listFiles();
        for (File dataRegionDir : Objects.requireNonNull(dataRegionDirs)) {
          if (!checkIsDirectory(dataRegionDir)) {
            continue;
          }
          // get time partition dirs and sort them
          List<File> timePartitionDirs =
              Arrays.asList(Objects.requireNonNull(dataRegionDir.listFiles())).stream()
                  .filter(file -> Pattern.compile("[0-9]*").matcher(file.getName()).matches())
                  .collect(Collectors.toList());
          timePartitionDirs.sort(
              (f1, f2) ->
                  Long.compareUnsigned(Long.parseLong(f1.getName()), Long.parseLong(f2.getName())));
          for (File timePartitionDir : Objects.requireNonNull(timePartitionDirs)) {
            if (!checkIsDirectory(timePartitionDir)) {
              continue;
            }
            // get all seq files under the time partition dir
            List<File> tsFiles =
                Arrays.asList(
                    Objects.requireNonNull(
                        timePartitionDir.listFiles(
                            file -> file.getName().endsWith(TSFILE_SUFFIX))));
            // sort the seq files with timestamp
            tsFiles.sort(
                (f1, f2) -> {
                  int timeDiff =
                      Long.compareUnsigned(
                          Long.parseLong(f1.getName().split("-")[0]),
                          Long.parseLong(f2.getName().split("-")[0]));
                  return timeDiff == 0
                      ? Long.compareUnsigned(
                          Long.parseLong(f1.getName().split("-")[1]),
                          Long.parseLong(f2.getName().split("-")[1]))
                      : timeDiff;
                });

            findIncorrectFiles(tsFiles);
          }
          // clear map
          clearMap(false);
        }
      }
    }
    if (printDetails) {
      printBoth("Finish checking successfully, totally find " + getBadFileNum() + " bad files.");
    }
    if (printToFile) {
      pw.close();
    }
  }

  public static void findIncorrectFiles(List<File> tsFiles) {
    for (File tsFile : tsFiles) {
      validationScan.getPreviousBadFileMsgs().clear();
      validationScan.scanTsFile(tsFile);
      for (String msg : validationScan.getPreviousBadFileMsgs()) {
        printBoth(msg);
      }
    }
  }

  private static boolean checkArgs(String[] args) {
    if (args.length < 1) {
      System.out.println(
          "Please input correct param, which is [path of data dir] [-pd = print details or not] [-f = path of outFile]. Eg: xxx/iotdb/data/data -pd=true -f=xxx/TsFile_validation_view.txt");
      return false;
    } else {
      for (String arg : args) {
        if (arg.startsWith("-pd")) {
          printDetails = Boolean.parseBoolean(arg.split("=")[1]);
        } else if (arg.startsWith("-if")) {
          ignoreFileOverlap = Boolean.parseBoolean(arg.split("=")[1]);
        } else if (arg.startsWith("-f")) {
          printToFile = true;
          outFilePath = arg.split("=")[1];
        } else {
          File f = new File(arg);
          if (f.isDirectory()
              && Objects.requireNonNull(
                          f.list(
                              (dir, name) ->
                                  (name.equals("sequence") || name.equals("unsequence"))))
                      .length
                  == 2) {
            File seqDataDir = new File(f, "sequence");
            seqDataDirList.add(seqDataDir);
          } else if (arg.endsWith(TSFILE_SUFFIX) && f.isFile()) {
            fileList.add(f);
          } else {
            System.out.println(arg + " is not a correct data directory or tsfile of IOTDB.");
            return false;
          }
        }
      }
      if (seqDataDirList.isEmpty() && fileList.isEmpty()) {
        System.out.println(
            "Please input correct param, which is [path of data dir] [-pd = print details or not] [-f = path of outFile]. Eg: xxx/iotdb/data/data -pd=true -f=xxx/TsFile_validation_view.txt");
        return false;
      }
      return true;
    }
  }

  public static void clearMap(boolean resetBadFileNum) {
    validationScan.reset(resetBadFileNum);
  }

  private static boolean checkIsDirectory(File dir) {
    boolean res = true;
    if (!dir.isDirectory()) {
      logger.error("{} is not a directory or does not exist, skip it.", dir.getAbsolutePath());
      res = false;
    }
    return res;
  }

  private static void printBoth(String msg) {
    System.out.println(msg);
    if (printToFile) {
      pw.println(msg);
    }
  }

  public static int getBadFileNum() {
    return validationScan.getBadFileNum();
  }

  public static void setBadFileNum(int badFileNum) {
    validationScan.setBadFileNum(badFileNum);
  }
}
