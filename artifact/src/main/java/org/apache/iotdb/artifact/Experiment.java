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

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Experiment {

  private static int MAX_ROW_NUM = 8192;
  private static String TEMP_FILE = "temp";
  private static String DEVICE = "root.group0.d0";
  private static String SENSOR = "s1";

  private long space = 0;
  private long encodeTime = 0;
  private long decodeTime = 0;

  public Experiment() {}

  public void test(DoubleArrayList data, TSEncoding encoding) {
    try {
      writeTest(data, encoding);
      DoubleArrayList decoded = readTest();
      assert check(data, decoded);
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
    }
  }

  private void writeTest(DoubleArrayList data, TSEncoding encoding)
      throws IOException, WriteProcessException {
    File f = new File(TEMP_FILE);
    TsFileWriter writer = new TsFileWriter(f);
    List<MeasurementSchema> measurements = new ArrayList<>();
    measurements.add(
        new MeasurementSchema(SENSOR, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED));
    writer.registerTimeseries(new Path(DEVICE), measurements);
    PageWriter.encodeTimeCost = 0;
    Tablet tablet = new Tablet(DEVICE, measurements, MAX_ROW_NUM);
    for (int i = 0; i < data.size(); i++) {
      tablet.rowSize++;
      tablet.addTimestamp(i % MAX_ROW_NUM, i);
      tablet.addValue(SENSOR, i % MAX_ROW_NUM, data.get(i));
      if (tablet.rowSize == MAX_ROW_NUM) {
        writer.write(tablet);
        tablet.reset();
      }
    }
    if (tablet.rowSize > 0) {
      writer.write(tablet);
      tablet.reset();
    }
    writer.close();
    this.space = f.length();
    this.encodeTime = PageWriter.encodeTimeCost;
  }

  private DoubleArrayList readTest() throws IOException {
    DoubleArrayList data = new DoubleArrayList();
    TsFileReader reader = new TsFileReader(new TsFileSequenceReader(TEMP_FILE));
    Path path = new Path(DEVICE, SENSOR);
    List<Path> paths = new ArrayList<>();
    paths.add(path);
    PageReader.decodeTimeCost = 0;
    QueryDataSet dataSet = reader.query(QueryExpression.create(paths, null));
    while (dataSet.hasNext()) {
      data.add(dataSet.next().getFields().get(0).getDoubleV());
    }
    this.decodeTime += PageReader.decodeTimeCost;
    return data;
  }

  private boolean check(DoubleArrayList a, DoubleArrayList b) {
    if (a.size() != b.size()) {
      return false;
    }
    boolean ans = true;
    for (int i = 0; i < a.size(); i++) {
      if (a.get(i) != b.get(i)) {
        ans = false;
      }
    }
    return ans;
  }

  public long getSpace() {
    return space;
  }

  public long getEncodeTime() {
    return encodeTime;
  }

  public long getDecodeTime() {
    return decodeTime;
  }
}
