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
package org.apache.iotdb.tsfile.test1831;

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

  private static final String DEVICE1 = "device_";
  public static int chunkNum;
  public static int fileNum = 500;

  public static void main(String[] args) throws IOException {
    long costTime = 0L;
    long totalStartTime = System.nanoTime();
    Options opts = new Options();
    Option chunkNumOption =
        OptionBuilder.withArgName("args").withLongOpt("chunkNum").hasArg().create("c");
    opts.addOption(chunkNumOption);

    BasicParser parser = new BasicParser();
    CommandLine cl;
    try {
      cl = parser.parse(opts, args);
      chunkNum = Integer.parseInt(cl.getOptionValue("c"));
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      // file path
      String path =
          "/home/fit/szs/data/data/sequence/root.sg/0/"
              + chunkNum
              + "/test"
              + fileIndex
              + ".tsfile";

      // raw data query
      try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
          ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {

        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path(DEVICE1, "sensor_1"));

        QueryExpression queryExpression = QueryExpression.create(paths, null);

        long startTime = System.nanoTime();
        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        while (queryDataSet.hasNext()) {
          queryDataSet.next();
        }

        costTime += (System.nanoTime() - startTime);
      }
    }
    System.out.println(
        "Total raw read cost time: " + (System.nanoTime() - totalStartTime) / 1000_000 + "ms");
    System.out.println("Index area cost time: " + costTime / 1000_000 + "ms");
  }
}
