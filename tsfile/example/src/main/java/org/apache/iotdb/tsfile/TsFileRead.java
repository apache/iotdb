/**
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
package org.apache.iotdb.tsfile;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * The class is to show how to read TsFile file named "test.tsfile".
 * The TsFile file "test.tsfile" is generated from class TsFileWrite2 or class TsFileWrite,
 * they generate the same TsFile file by two different ways
 * <p>
 * Run TsFileWrite1 or TsFileWrite to generate the test.tsfile first
 */
public class TsFileRead {

  public static void main(String[] args) throws IOException {

    // file path
    String path = "test.tsfile";

    // read example : no filter
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_1.sensor_2"));
    paths.add(new Path("device_1.sensor_3"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
    reader.close();

    // time filter : 4 <= time <= 10
    IExpression timeFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(4L)),
        new GlobalTimeExpression(TimeFilter.ltEq(10L)));
    reader = new TsFileSequenceReader(path);
    readTsFile = new ReadOnlyTsFile(reader);
    paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_1.sensor_2"));
    paths.add(new Path("device_1.sensor_3"));
    queryExpression = QueryExpression.create(paths, timeFilter);
    queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
    reader.close();

    // value filter : device_1.sensor_2 <= 20
    IExpression valueFilter = new SingleSeriesExpression(new Path("device_1.sensor_2"),
        ValueFilter.ltEq(20));
    reader = new TsFileSequenceReader(path);
    readTsFile = new ReadOnlyTsFile(reader);
    paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_1.sensor_2"));
    paths.add(new Path("device_1.sensor_3"));
    queryExpression = QueryExpression.create(paths, valueFilter);
    queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
    reader.close();

    // time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20
    timeFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(4L)),
        new GlobalTimeExpression(TimeFilter.ltEq(10L)));
    valueFilter = new SingleSeriesExpression(new Path("device_1.sensor_3"), ValueFilter.gtEq(20));
    reader = new TsFileSequenceReader(path);
    readTsFile = new ReadOnlyTsFile(reader);
    paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_1.sensor_2"));
    paths.add(new Path("device_1.sensor_3"));
    IExpression finalFilter = BinaryExpression.and(timeFilter, valueFilter);
    queryExpression = QueryExpression.create(paths, finalFilter);
    queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    reader.close();
  }

}