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

package org.apache.tsfile.read;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class TsFileReaderTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader fileReader;
  private TsFileReader tsFile;

  @Test
  public void multiPagesTest() throws IOException, WriteProcessException {
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    // make multi pages in one group
    int oldPointNumInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    int oldGroupSizeInByte = tsFileConfig.getGroupSizeInByte();
    tsFileConfig.setMaxNumberOfPointsInPage(100);
    tsFileConfig.setGroupSizeInByte(100 * 1024 * 1024);
    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);

    Path path = new Path("t", "id", true);
    tsFileWriter.registerTimeseries(
        new Path(path.getIDeviceID()),
        new MeasurementSchema("id", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));

    for (int i = 0; i < 11000000; i++) {
      TSRecord t = new TSRecord("t", i);
      if (i % 100 == 0) {
        // Add a large max_value to the page statistics,
        // and get a very large number of invalid pages when the query is executed
        t.addTuple(new IntDataPoint("id", 9000001));
      } else {
        t.addTuple(new IntDataPoint("id", i));
      }
      tsFileWriter.writeRecord(t);
    }
    // make same value to filter
    TSRecord t = new TSRecord("t", 101011000000L);
    t.addTuple(new IntDataPoint("id", 8000001));
    tsFileWriter.writeRecord(t);
    tsFileWriter.flush();
    tsFileWriter.close();

    TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath));

    SingleSeriesExpression filter =
        new SingleSeriesExpression(
            path, ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 8000001, TSDataType.INT32));
    QueryExpression queryExpression = QueryExpression.create(Arrays.asList(path), filter);
    QueryDataSet query = tsFileReader.query(queryExpression);

    int i = 0;
    Assert.assertTrue(query.hasNext());
    while (query.hasNext()) {
      RowRecord next = query.next();
      if (i == 0) {
        Assert.assertEquals(next.getTimestamp(), 8000001);
        Assert.assertEquals(next.getFields().get(0).getIntV(), 8000001);
        i++;
      } else {
        Assert.assertEquals(next.getTimestamp(), 101011000000L);
        Assert.assertEquals(next.getFields().get(0).getIntV(), 8000001);
      }
    }

    tsFileReader.close();
    file.delete();
    tsFileConfig.setGroupSizeInByte(oldGroupSizeInByte);
    tsFileConfig.setMaxNumberOfPointsInPage(oldPointNumInPage);
  }

  @Test
  public void test1() throws IOException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    int rowCount = 1000;
    TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new TsFileReader(fileReader);
    queryTest(rowCount);
    tsFile.close();
    TsFileGeneratorForTest.after();
  }

  private void queryTest(int rowCount) throws IOException {
    Filter filter = TimeFilterApi.lt(1480562618100L);
    Filter filter2 =
        ValueFilterApi.gt(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("dog", TSFileConfig.STRING_CHARSET),
            TSDataType.TEXT);
    Filter filter3 =
        FilterFactory.and(TimeFilterApi.gtEq(1480562618000L), TimeFilterApi.ltEq(1480562618100L));

    IExpression IExpression =
        BinaryExpression.or(
            BinaryExpression.and(
                new SingleSeriesExpression(new Path("d1", "s1", true), filter),
                new SingleSeriesExpression(new Path("d1", "s4", true), filter2)),
            new GlobalTimeExpression(filter3));

    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s4", true))
            .setExpression(IExpression);
    QueryDataSet queryDataSet = tsFile.query(queryExpression);
    long aimedTimestamp = 1480562618000L;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
      aimedTimestamp++;
    }

    queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s4", true));
    queryDataSet = tsFile.query(queryExpression);
    aimedTimestamp = 1480562618000L;
    int count = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
      aimedTimestamp++;
      count++;
    }
    Assert.assertEquals(rowCount, count);

    queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s4", true))
            .setExpression(new GlobalTimeExpression(filter3));
    queryDataSet = tsFile.query(queryExpression);
    aimedTimestamp = 1480562618000L;
    count = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
      aimedTimestamp++;
      count++;
    }
    Assert.assertEquals(101, count);
  }

  @Test
  public void test2() throws Exception {
    int minRowCount = 1000, maxRowCount = 100000;
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    TsFileGeneratorForTest.generateFile(minRowCount, maxRowCount, 16 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new TsFileReader(fileReader);
    queryTest2();
    queryNonExistPathTest();
    tsFile.close();
    TsFileGeneratorForTest.after();
  }

  void queryTest2() throws IOException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6", true));
    paths.add(new Path("d2", "s1", true));

    IExpression expression = new GlobalTimeExpression(TimeFilterApi.gt(1480562664760L));

    QueryExpression queryExpression = QueryExpression.create(paths, expression);

    QueryDataSet queryDataSet = tsFile.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
      cnt++;
    }
    Assert.assertEquals(10647, cnt);
  }

  void queryNonExistPathTest() throws Exception {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s1", true));
    paths.add(new Path("d2", "s1", true));
    IExpression expression = new GlobalTimeExpression(TimeFilterApi.gt(1480562664760L));
    QueryExpression queryExpression = QueryExpression.create(paths, expression);
    try {
      QueryDataSet queryDataSet = tsFile.query(queryExpression);
    } catch (Exception e) {
      throw new Exception(e);
      // fail();
    }
  }

  @Test
  public void queryWithoutFilter() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(10, 100, 30);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s2", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s1", true));

      long rowCount = queryAndPrint(paths, tsFileReader, null);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithTimeFilter() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s2", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s2", true));

      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(29990L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(30009L)));
      long rowCount = queryAndPrint(paths, tsFileReader, timeFilter);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithValueFilter() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s2", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s2", true));

      IExpression valueFilter =
          new SingleSeriesExpression(
              new Path("d2", "s1", true),
              ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 9L, TSDataType.INT64));
      long rowCount = queryAndPrint(paths, tsFileReader, valueFilter);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithValueFilter2() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s2", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s1", true));

      IExpression valueFilter1 =
          new SingleSeriesExpression(
              new Path("d2", "s1", true),
              ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64));
      IExpression valueFilter2 =
          new SingleSeriesExpression(
              new Path("d1", "s2", true),
              ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10000L, TSDataType.INT64));
      IExpression binaryExpression = BinaryExpression.and(valueFilter1, valueFilter2);
      long rowCount = queryAndPrint(paths, tsFileReader, binaryExpression);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithAndBinaryFilter() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s2", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s1", true));

      IExpression valueFilter =
          BinaryExpression.and(
              new SingleSeriesExpression(
                  new Path("d2", "s1", true),
                  ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 7000L, TSDataType.INT64)),
              new SingleSeriesExpression(
                  new Path("d1", "s1", true),
                  ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10000L, TSDataType.INT64)));
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(2000)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(10000L)));
      IExpression binaryExpression = BinaryExpression.and(valueFilter, timeFilter);
      long rowCount = queryAndPrint(paths, tsFileReader, binaryExpression);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithOrBinaryFilter() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s2", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s1", true));

      IExpression valueFilter1 =
          new SingleSeriesExpression(
              new Path("d2", "s1", true),
              ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64));
      IExpression valueFilter2 =
          new SingleSeriesExpression(
              new Path("d1", "s2", true),
              ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10000L, TSDataType.INT64));
      IExpression valueFilter = BinaryExpression.and(valueFilter1, valueFilter2);
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(19990L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(18009L)));
      IExpression binaryExpression = BinaryExpression.or(valueFilter, timeFilter);
      long rowCount = queryAndPrint(paths, tsFileReader, binaryExpression);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithAndBinaryFilter2() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s3", true));
      paths.add(new Path("d2", "s1", true));
      paths.add(new Path("d2", "s2", true));

      IExpression valueFilter1 =
          new SingleSeriesExpression(
              new Path("d2", "s1", true),
              ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64));
      IExpression valueFilter2 =
          new SingleSeriesExpression(
              new Path("d1", "s2", true),
              ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10000L, TSDataType.INT64));
      IExpression valueFilter = BinaryExpression.and(valueFilter1, valueFilter2);
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(1799L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(1900L)));
      IExpression binaryExpression = BinaryExpression.and(valueFilter, timeFilter);
      long rowCount = queryAndPrint(paths, tsFileReader, binaryExpression);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithUnRegisteredTimeseries() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s9", true));
      paths.add(new Path("d2", "s1", true));
      paths.add(new Path("d2", "s8", true));
      paths.add(new Path("d9", "s8", true));

      IExpression valueFilter1 =
          new SingleSeriesExpression(
              new Path("d2", "s1", true),
              ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64));
      IExpression valueFilter2 =
          new SingleSeriesExpression(
              new Path("d1", "s2", true),
              ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10000L, TSDataType.INT64));
      IExpression valueFilter = BinaryExpression.and(valueFilter1, valueFilter2);
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(1799L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(1900L)));
      IExpression binaryExpression = BinaryExpression.and(valueFilter, timeFilter);
      long rowCount = queryAndPrint(paths, tsFileReader, binaryExpression);
      Assert.assertNotEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void queryWithValueFilterOnUnExistedTimeseries() throws IOException {
    TsFileGeneratorForTest.generateAlignedTsFile(100000, 1024, 100);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(filePath)); ) {
      // timeseries path for query
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1", true));
      paths.add(new Path("d1", "s9", true));
      paths.add(new Path("d2", "s1", true));
      paths.add(new Path("d9", "s8", true));

      IExpression valueFilter1 =
          new SingleSeriesExpression(
              new Path("d2", "s9", true),
              ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64));
      IExpression valueFilter2 =
          new SingleSeriesExpression(
              new Path("d1", "s2", true),
              ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10000L, TSDataType.INT64));
      IExpression valueFilter = BinaryExpression.and(valueFilter1, valueFilter2);
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.gtEq(1799L)),
              new GlobalTimeExpression(TimeFilterApi.ltEq(1900L)));
      IExpression binaryExpression = BinaryExpression.and(valueFilter, timeFilter);
      long rowCount = queryAndPrint(paths, tsFileReader, binaryExpression);
      Assert.assertEquals(0, rowCount);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  private static long queryAndPrint(
      ArrayList<Path> paths, TsFileReader readTsFile, IExpression statement) throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    long rowCount = 0;
    while (queryDataSet.hasNext()) {
      // System.out.println(queryDataSet.next());
      queryDataSet.next();
      rowCount++;
    }
    return rowCount;
  }

  @Test
  public void testGetAlignedChunkMetadata() throws IOException {
    // generate aligned timeseries "d1.s1","d1.s2","d1.s3","d1.s4" and nonAligned timeseries
    // "d2.s1","d2.s2","d2.s3"
    TsFileGeneratorForTest.generateAlignedTsFile(10, 100, 30);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      // query for non-exist device
      IDeviceID d3 = IDeviceID.Factory.DEFAULT_FACTORY.create("d3");
      try {
        reader.getAlignedChunkMetadata(d3, true);
      } catch (IOException e) {
        Assert.assertEquals("Device {" + d3 + "} is not in tsFileMetaData", e.getMessage());
      }

      // query for non-aligned device
      IDeviceID d2 = IDeviceID.Factory.DEFAULT_FACTORY.create("d2");
      try {
        reader.getAlignedChunkMetadata(d2, true);
      } catch (IOException e) {
        Assert.assertEquals("Timeseries of device {" + d2 + "} are not aligned", e.getMessage());
      }

      String[] expected = new String[] {"s1", "s2", "s3", "s4"};

      List<AlignedChunkMetadata> chunkMetadataList =
          reader.getAlignedChunkMetadata(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), true);
      AlignedChunkMetadata alignedChunkMetadata = chunkMetadataList.get(0);
      Assert.assertEquals("", alignedChunkMetadata.getTimeChunkMetadata().getMeasurementUid());
      int i = 0;
      for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        Assert.assertEquals(expected[i], chunkMetadata.getMeasurementUid());
        i++;
      }

      Assert.assertEquals(expected.length, i);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }

  @Test
  public void testGetDeviceMethods() throws IOException, WriteProcessException {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.testsg", 0, 0, 0);
    try {
      File file = TsFileGeneratorUtils.generateAlignedTsFile(filePath, 5, 1, 10, 1, 1, 10, 100);
      try (TsFileReader tsFileReader = new TsFileReader(file)) {
        Assert.assertEquals(
            Arrays.asList(
                "root.testsg.d10000",
                "root.testsg.d10001",
                "root.testsg.d10002",
                "root.testsg.d10003",
                "root.testsg.d10004"),
            tsFileReader.getAllDevices());
        List<IMeasurementSchema> timeseriesSchema =
            tsFileReader.getTimeseriesSchema("root.testsg.d10000");
        Assert.assertEquals(2, timeseriesSchema.size());
        Assert.assertEquals("", timeseriesSchema.get(0).getMeasurementName());
        Assert.assertEquals("s0", timeseriesSchema.get(1).getMeasurementName());
      }
    } finally {
      Files.deleteIfExists(Paths.get(filePath));
    }
  }

  @Test
  public void testGetTableDeviceMethods() throws IOException, WriteProcessException {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.testsg", 0, 0, 0);
    try {
      File file = TsFileGeneratorUtils.generateAlignedTsFile(filePath, 5, 1, 10, 1, 1, 10, 100);
      List<IDeviceID> deviceIDList = new ArrayList<>();
      TableSchema tableSchema =
          new TableSchema(
              "t1",
              Arrays.asList(
                  new MeasurementSchema("id1", TSDataType.STRING),
                  new MeasurementSchema("id2", TSDataType.STRING),
                  new MeasurementSchema("id3", TSDataType.STRING),
                  new MeasurementSchema("s1", TSDataType.INT32)),
              Arrays.asList(
                  Tablet.ColumnCategory.ID,
                  Tablet.ColumnCategory.ID,
                  Tablet.ColumnCategory.ID,
                  Tablet.ColumnCategory.MEASUREMENT));
      try (TsFileWriter writer = new TsFileWriter(file)) {
        writer.registerTableSchema(tableSchema);
        Tablet tablet =
            new Tablet(
                tableSchema.getTableName(),
                IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
                IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
                tableSchema.getColumnTypes());

        String[][] ids =
            new String[][] {
              {null, null, null},
              {null, null, "id3-4"},
              {null, "id2-1", "id3-1"},
              {null, "id2-5", null},
              {"id1-2", null, "id3-2"},
              {"id1-3", "id2-3", null},
              {"id1-6", null, null},
            };
        for (int i = 0; i < ids.length; i++) {
          tablet.addTimestamp(i, i);
          tablet.addValue("id1", i, ids[i][0]);
          tablet.addValue("id2", i, ids[i][1]);
          tablet.addValue("id3", i, ids[i][2]);
          deviceIDList.add(
              new StringArrayDeviceID(tableSchema.getTableName(), ids[i][0], ids[i][1], ids[i][2]));
          tablet.addValue("s1", i, i);
        }
        tablet.setRowSize(ids.length);
        writer.writeTable(tablet);
      }
      try (TsFileReader tsFileReader = new TsFileReader(file)) {
        Assert.assertEquals(
            new HashSet<>(deviceIDList), new HashSet<>(tsFileReader.getAllTableDevices("t1")));
        Assert.assertEquals("t1", tsFileReader.getAllTables().get(0));
        Assert.assertEquals(
            tableSchema, tsFileReader.getTableSchema(Collections.singletonList("t1")).get(0));
      }
    } finally {
      Files.deleteIfExists(Paths.get(filePath));
    }
  }
}
