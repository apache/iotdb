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

package org.apache.tsfile;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.tsfile.Constant.DEVICE_1;
import static org.apache.tsfile.Constant.SENSOR_1;
import static org.apache.tsfile.Constant.SENSOR_2;
import static org.apache.tsfile.Constant.SENSOR_3;
import static org.apache.tsfile.Constant.SENSOR_4;
import static org.apache.tsfile.Constant.SENSOR_5;
import static org.apache.tsfile.Constant.SENSOR_6;
import static org.apache.tsfile.Constant.SENSOR_7;

/**
 * The class is to show how to read TsFile file named "test.tsfile". The TsFile file "test.tsfile"
 * is generated from class TsFileWriteWithTSRecord or TsFileWriteWithTablet. Run
 * TsFileWriteWithTSRecord or TsFileWriteWithTablet to generate the test.tsfile first
 */
public class TsFileRead {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileRead.class);

  private static void queryAndPrint(
      ArrayList<Path> paths, TsFileReader readTsFile, IExpression statement) throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      String next = queryDataSet.next().toString();
      LOGGER.info(next);
    }
    LOGGER.info("----------------");
  }

  public static void main(String[] args) throws IOException {

    // file path
    String path = "test.tsfile";

    // create reader and get the readTsFile interface
    try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
        TsFileReader readTsFile = new TsFileReader(reader)) {

      // use these paths(all measurements) for all the queries
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(DEVICE_1, SENSOR_1, true));
      paths.add(new Path(DEVICE_1, SENSOR_2, true));
      paths.add(new Path(DEVICE_1, SENSOR_3, true));
      paths.add(new Path(DEVICE_1, SENSOR_4, true));
      paths.add(new Path(DEVICE_1, SENSOR_5, true));
      paths.add(new Path(DEVICE_1, SENSOR_6, true));
      paths.add(new Path(DEVICE_1, SENSOR_7, true));

      // no filter, should select 1 2 3 4 6 7 8
      queryAndPrint(paths, readTsFile, null);

      // time filter : 4 <= time <= 10, should select 4 6 7 8
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(4L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(10L)));
      queryAndPrint(paths, readTsFile, timeFilter);

      // value filter : device_1.sensor_2 <= 20, should select 1 2 4 6 7
      IExpression valueFilter =
          new SingleSeriesExpression(new Path(DEVICE_1, SENSOR_2, true), ValueFilterApi.ltEq(20L));
      queryAndPrint(paths, readTsFile, valueFilter);

      // time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20, should select 4 7 8
      timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(4L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(10L)));
      valueFilter =
          new SingleSeriesExpression(new Path(DEVICE_1, SENSOR_3, true), ValueFilterApi.gtEq(20L));
      IExpression finalFilter = BinaryExpression.and(timeFilter, valueFilter);
      queryAndPrint(paths, readTsFile, finalFilter);
    }
  }
}
