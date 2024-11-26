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
package org.apache.tsfile.read.query.timegenerator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class ReadWriteTest {

  private final String TEMPLATE_NAME = "template";
  private final String tsfilePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);

  @Before
  public void before() throws IOException, WriteProcessException {
    writeFile(tsfilePath);
  }

  @After
  public void after() {
    File file = new File(tsfilePath);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testFilterAnd() throws IOException {
    Filter timeFilter = FilterFactory.and(TimeFilterApi.gtEq(1L), TimeFilterApi.ltEq(8L));
    IExpression timeExpression = new GlobalTimeExpression(timeFilter);

    IExpression valueExpression =
        BinaryExpression.and(
            new SingleSeriesExpression(
                new Path("d1", "s1", true),
                ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 1.0f, TSDataType.FLOAT)),
            new SingleSeriesExpression(
                new Path("d1", "s2", true),
                ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 22, TSDataType.INT32)));

    IExpression finalExpression = BinaryExpression.and(valueExpression, timeExpression);

    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s2", true))
            .setExpression(finalExpression);

    try (TsFileSequenceReader fileReader = new TsFileSequenceReader(tsfilePath)) {
      TsFileReader tsFileReader = new TsFileReader(fileReader);
      QueryDataSet dataSet = tsFileReader.query(queryExpression);
      int i = 0;
      String[] expected =
          new String[] {"1\t1.2\t20", "3\t1.4\t21", "4\t1.2\t20", "6\t7.2\t10", "7\t6.2\t20"};
      while (dataSet.hasNext()) {
        Assert.assertEquals(expected[i], dataSet.next().toString());
        i++;
      }
      Assert.assertEquals(5, i);
    }
  }

  private void writeFile(String tsfilePath) throws IOException, WriteProcessException {
    File f = new File(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    if (!f.getParentFile().exists()) {
      Assert.assertTrue(f.getParentFile().mkdirs());
    }
    Schema schema = new Schema();
    schema.extendTemplate(
        TEMPLATE_NAME, new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        TEMPLATE_NAME, new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        TEMPLATE_NAME, new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(f, schema);

    IDeviceID d1 = IDeviceID.Factory.DEFAULT_FACTORY.create("d1");
    tsFileWriter.registerTimeseries(d1, new MeasurementSchema("s1", TSDataType.FLOAT));
    tsFileWriter.registerTimeseries(d1, new MeasurementSchema("s2", TSDataType.INT32));

    // construct TSRecord
    TSRecord tsRecord = new TSRecord("d1", 1);
    DataPoint dPoint1 = new FloatDataPoint("s1", 1.2f);
    DataPoint dPoint2 = new IntDataPoint("s2", 20);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.writeRecord(tsRecord);

    tsRecord = new TSRecord("d1", 2);
    dPoint2 = new IntDataPoint("s2", 20);
    DataPoint dPoint3 = new IntDataPoint("s3", 50);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);

    tsRecord = new TSRecord("d1", 3);
    dPoint1 = new FloatDataPoint("s1", 1.4f);
    dPoint2 = new IntDataPoint("s2", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.writeRecord(tsRecord);

    tsRecord = new TSRecord("d1", 4);
    dPoint1 = new FloatDataPoint("s1", 1.2f);
    dPoint2 = new IntDataPoint("s2", 20);
    dPoint3 = new IntDataPoint("s3", 51);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);

    tsRecord = new TSRecord("d1", 6);
    dPoint1 = new FloatDataPoint("s1", 7.2f);
    dPoint2 = new IntDataPoint("s2", 10);
    dPoint3 = new IntDataPoint("s3", 11);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);

    tsRecord = new TSRecord("d1", 7);
    dPoint1 = new FloatDataPoint("s1", 6.2f);
    dPoint2 = new IntDataPoint("s2", 20);
    dPoint3 = new IntDataPoint("s3", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);

    tsRecord = new TSRecord("d1", 8);
    dPoint1 = new FloatDataPoint("s1", 9.2f);
    dPoint2 = new IntDataPoint("s2", 30);
    dPoint3 = new IntDataPoint("s3", 31);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);

    // close TsFile
    tsFileWriter.close();
  }
}
