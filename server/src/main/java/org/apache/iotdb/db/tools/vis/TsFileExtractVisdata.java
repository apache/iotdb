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

package org.apache.iotdb.db.tools.vis;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extract, from input tsfiles, necessary visualization information, which is what "vis.m" needs to
 * plot figures.
 *
 * <p>Input: [path1 seqIndicator1 path2 seqIndicator2 ... pathN seqIndicatorN outputPath]
 *
 * <p>Example: G:\\debug\\data\\sequence true G:\\debug\\data\\unsequence false visdata.csv 2N+1
 * args in total.
 *
 * <p>`seqIndicator` should be 'true' or 'false' (not case sensitive). 'true' means is the file is
 * sequence, 'false' means the file is unsequence.
 *
 * <p>`Path` can be the full path of a file or a directory path. If it is a directory path, make
 * sure that all files in this directory have the same seqIndicator.
 *
 * <p>All input TsFiles should be sealed. The handle of unsealed TsFile is left as future work when
 * in need.
 *
 * <p>Output content format: [tsName, fileName, chunkId, startTime, endTime, pointCountNum]
 *
 * <p>`fileName`: If the tsfile is unsequence file, TsFileExtractVisdata will make sure that the
 * fileName contains "unseq" as an indicator which will be used by "vis.m".
 *
 * <p>NOTE: There is no version info stored in TsFileV3/IoTDBV0.12 any more. As a result,
 * TsFileExtractVisdata (1) will not extract version info as it does to TsFileV2/IoTDBV0.11 and (2)
 * will not use reader.selfCheck to get ChunkMetadata, which will be fetched by reading metadata at
 * the end of the TsFile instead. That means, the input TsFiles should all be sealed. The handle of
 * unsealed TsFile is left as future work when in need.
 */
public class TsFileExtractVisdata {

  public static String seqFileNameSuffix = "(seque)";
  public static String unseqFileNameSuffix = "(unseq)";

  public static void main(String[] args) throws IOException {
    int M = args.length;
    if (M % 2 == 0) {
      throw new IOException(
          "2N+1 args should be:[path1 seqIndicator1 path2 seqIndicator2 "
              + "... pathN seqIndicatorN outputPath]");
    }
    List<String> inputPathList = new ArrayList<>();
    List<Boolean> seqIndicatorList = new ArrayList<>();
    for (int i = 0; i < M - 1; i = i + 2) {
      inputPathList.add(args[i]);
      String indicator = args[i + 1].toLowerCase();
      if (!indicator.equals("true") && !indicator.equals("false")) {
        throw new IOException("seqIndicator should be 'true' or 'false' (not case sensitive).");
      }
      seqIndicatorList.add(Boolean.parseBoolean(args[i + 1]));
    }
    String outputPath = args[M - 1];

    try (PrintWriter pw = new PrintWriter(new FileWriter(outputPath))) {
      int idx = 0;
      for (String inputPath : inputPathList) {
        boolean isSeq = seqIndicatorList.get(idx++);
        List<String> filelist = new ArrayList<>();
        filelist = getFile(inputPath, filelist); // get all tsfile paths under the inputPath
        for (String f : filelist) {
          System.out.println(f); // note that this info need not be written to outputFile
          String fileNameForVis;
          // Extract the file name from f, following the rule negotiated with "vis.m".
          // The rule is that if it is an unsequence tsfile, its extracted fileName must contain
          // "unseq".
          // It's not a must for the extracted fileName of a sequence tsfile to contain "seque", but
          // good to do so.
          File file = new File(f);
          String fileName = file.getName();
          if (isSeq) {
            fileNameForVis = fileName + seqFileNameSuffix;
          } else {
            fileNameForVis = fileName + unseqFileNameSuffix;
          }
          // extract necessary vis info from the tsfile
          try (TsFileSequenceReader reader = new TsFileSequenceReader(f)) {
            // get all ChunkMetadatas at the end of the TsFile
            for (String device : reader.getAllDevices()) {
              Map<String, List<ChunkMetadata>> seriesMetaData =
                  reader.readChunkMetadataInDevice(device);
              for (Map.Entry<String, List<ChunkMetadata>> series : seriesMetaData.entrySet()) {
                // chunkId is used by vis.m to differentiate different chunks of the same timeseries
                // in the same TsFile.
                long chunkId = 0;
                for (ChunkMetadata chunkMetadata : series.getValue()) {
                  String tsName = device + TsFileConstant.PATH_SEPARATOR + series.getKey();
                  long startTime = chunkMetadata.getStartTime();
                  long endTime = chunkMetadata.getEndTime();
                  long pointCountNum = chunkMetadata.getStatistics().getCount();
                  chunkId++;
                  printlnBoth(
                      pw,
                      String.format(
                          "%s,%s,%d,%d,%d,%d",
                          tsName, fileNameForVis, chunkId, startTime, endTime, pointCountNum));
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Get the list of paths of all tsfiles under the input path recursively.
   *
   * @param path can be the full path of a file or a directory path
   * @param filelist
   * @return the list of paths of all tsfiles under the input path
   */
  private static List<String> getFile(String path, List<String> filelist) {
    File file = new File(path);
    File[] array = file.listFiles();
    if (array == null) {
      filelist.add(path);
      return filelist;
    }
    for (File value : array) {
      if (value.isFile()) {
        String filePath = value.getPath();
        if (filePath.endsWith(TsFileConstant.TSFILE_SUFFIX)) {
          // only include tsfiles
          filelist.add(value.getPath());
        }
      } else if (value.isDirectory()) {
        getFile(value.getPath(), filelist);
      }
    }
    return filelist;
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }
}
