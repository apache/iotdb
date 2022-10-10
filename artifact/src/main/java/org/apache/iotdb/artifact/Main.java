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
package org.apache.iotdb.artifact;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

import java.io.File;
import java.io.IOException;

public class Main {

  private static final String CSV_SUFFIX = ".csv";
  private static final String DATASET_PATH = "dataset";
  private static final int REPEAT_TIMES = 100;
  private static final int BLOCK_SIZE = 1024;
  private static final double BETA = 0;
  private static final TSEncoding[] ENCODINGS = {
    TSEncoding.PLAIN,
    TSEncoding.DESCEND,
    TSEncoding.GORILLA,
    TSEncoding.TS_2DIFF,
    TSEncoding.RLE,
    TSEncoding.BUFF
  };
  private static final String SPACE_RESULT_PATH = "result/space.csv";
  private static final String ENCODE_TIME_RESULT_PATH = "result/encode_time.csv";
  private static final String DECODE_TIME_RESULT_PATH = "result/decode_time.csv";

  public static void main(String[] args) throws IOException {

    // prepare datasets for encodings
    File dir = new File(DATASET_PATH);
    if (!dir.isDirectory()) {
      System.out.println("Cannot find directory artifact/dataset");
      return;
    }
    File[] files = dir.listFiles((s) -> s.getName().endsWith(CSV_SUFFIX));

    // variables for results
    double[][] space = new double[files.length][ENCODINGS.length];
    double[][] encodeTime = new double[files.length][ENCODINGS.length];
    double[][] decodeTime = new double[files.length][ENCODINGS.length];

    // traverse all datasets
    for (int i = 0; i < files.length; i++) {
      // transform to frequency domain
      DoubleArrayList timeDomain = Utils.loadFile(files[i]);
      DoubleArrayList frequencyDomain =
          ShortTimeFourierTransform.transform(timeDomain, BLOCK_SIZE, BETA);

      // traverse all encodings for multiple times and record the average results
      for (int j = 0; j < ENCODINGS.length; j++) {
        for (int k = 0; k < REPEAT_TIMES; k++) {
          Experiment experiment = new Experiment();
          experiment.test(frequencyDomain, ENCODINGS[j]);
          space[i][j] += experiment.getSpace();
          encodeTime[i][j] += experiment.getEncodeTime() * 1e-9;
          decodeTime[i][j] += experiment.getDecodeTime() * 1e-9;
        }
        space[i][j] /= REPEAT_TIMES;
        encodeTime[i][j] /= REPEAT_TIMES;
        decodeTime[i][j] /= REPEAT_TIMES;
      }
    }

    // print the results
    Utils.printTable(SPACE_RESULT_PATH, space, ENCODINGS, files);
    Utils.printTable(ENCODE_TIME_RESULT_PATH, encodeTime, ENCODINGS, files);
    Utils.printTable(DECODE_TIME_RESULT_PATH, decodeTime, ENCODINGS, files);
  }
}
