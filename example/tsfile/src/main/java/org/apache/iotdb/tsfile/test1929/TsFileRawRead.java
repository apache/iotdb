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
package org.apache.iotdb.tsfile.test1929;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;

public class TsFileRawRead {

  private static final String DEVICE1 = "device_1";
  public static int deviceNum;
  public static int sensorNum;
  public static int treeType; // 0=Zesong Tree, 1=B+ Tree
  public static int fileNum;

  public static long readFileMetadata;
  public static long readBuffer;
  public static long queryTime;

  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  public static void main(String[] args) throws IOException {
    Options opts = new Options();
    Option deviceNumOption =
        OptionBuilder.withArgName("args").withLongOpt("deviceNum").hasArg().create("d");
    opts.addOption(deviceNumOption);
    Option sensorNumOption =
        OptionBuilder.withArgName("args").withLongOpt("sensorNum").hasArg().create("m");
    opts.addOption(sensorNumOption);
    Option fileNumOption =
        OptionBuilder.withArgName("args").withLongOpt("fileNum").hasArg().create("f");
    opts.addOption(fileNumOption);
    Option treeTypeOption =
        OptionBuilder.withArgName("args").withLongOpt("treeType").hasArg().create("t");
    opts.addOption(treeTypeOption);
    Option degreeOption =
        OptionBuilder.withArgName("args").withLongOpt("degree").hasArg().create("c");
    opts.addOption(degreeOption);

    BasicParser parser = new BasicParser();
    CommandLine cl;
    try {
      cl = parser.parse(opts, args);
      deviceNum = Integer.parseInt(cl.getOptionValue("d"));
      sensorNum = Integer.parseInt(cl.getOptionValue("m"));
      fileNum = Integer.parseInt(cl.getOptionValue("f"));
      treeType = Integer.parseInt(cl.getOptionValue("t"));
      config.setMaxDegreeOfIndexNode(1024);
    } catch (Exception e) {
      e.printStackTrace();
    }

    long totalStartTime = System.nanoTime();
    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      String folder = treeType == 1 ? "root.b/" : "root.hash/";
      // file path
      String path =
          "/data/szs/data/data/sequence/"
              + folder
              + config.getMaxDegreeOfIndexNode()
              + "/"
              + deviceNum
              + "."
              + sensorNum
              + "/test"
              + fileIndex
              + ".tsfile";

      // raw data query
      try (TsFileSequenceReader reader = new TsFileSequenceReader(path, false);
          ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader, treeType)) {

        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path(DEVICE1, "sensor_1"));

        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression, treeType);
        while (queryDataSet.hasNext()) {
          queryDataSet.next();
        }
        readFileMetadata += reader.readFileMetadata;
        readBuffer += reader.readBuffer;
        queryTime += reader.queryTime;
      }
    }
    System.out.println("readBuffer: " + (double) readBuffer / (double) fileNum + "ms");
    System.out.println("readFileMetadata: " + (double) readFileMetadata / (double) fileNum + "ms");
    System.out.println("query time: " + (double) queryTime / (double) fileNum + "ms");
    long totalTime = (System.nanoTime() - totalStartTime) / 1000_000;
    System.out.println("Average cost time: " + (double) totalTime / (double) fileNum + "ms");
  }
}
