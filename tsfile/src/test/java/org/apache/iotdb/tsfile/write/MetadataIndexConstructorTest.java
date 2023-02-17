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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.tsfile.utils.FileGenerator.generateIndexString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** test for MetadataIndexConstructor */
public class MetadataIndexConstructorTest {
  private static final Logger logger = LoggerFactory.getLogger(MetadataIndexConstructorTest.class);
  private final TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("MetadataIndexConstructorTest.tsfile");

  private static final String measurementPrefix = "sensor_";
  private static final String vectorPrefix = "vector_";
  private int maxDegreeOfIndexNode;

  @Before
  public void before() {
    maxDegreeOfIndexNode = conf.getMaxDegreeOfIndexNode();
    conf.setMaxDegreeOfIndexNode(10);
  }

  @After
  public void after() {
    conf.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
    File file = new File(FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  /** Example 1: 5 entities with 5 measurements each */
  @Test
  public void singleIndexTest1() {
    int deviceNum = 5;
    int measurementNum = 5;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + i;
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] = measurementPrefix + generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 2: 1 entity with 150 measurements */
  @Test
  public void singleIndexTest2() {
    int deviceNum = 1;
    int measurementNum = 150;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + i;
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] = measurementPrefix + generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 3: 150 entities with 1 measurement each */
  @Test
  public void singleIndexTest3() {
    int deviceNum = 150;
    int measurementNum = 1;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + generateIndexString(i, deviceNum);
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] = measurementPrefix + generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 4: 150 entities with 150 measurements each */
  @Test
  public void singleIndexTest4() {
    int deviceNum = 150;
    int measurementNum = 1;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + generateIndexString(i, deviceNum);
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] = measurementPrefix + generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 5: 1 entities with 1 vector containing 9 measurements */
  @Test
  public void vectorIndexTest() {
    String[] devices = {"d0"};
    int[][] vectorMeasurement = {{9}};
    test(devices, vectorMeasurement, null);
  }

  /**
   * Example 6: 2 entities, measurements of entities are shown in the following table
   *
   * <p>d0.s0~s4 | d0.z0~z3 | d1.v0.(s0~s3)
   */
  @Test
  public void compositeIndexTest() {
    String[] devices = {"d0", "d1"};
    int[][] vectorMeasurement = {{}, {4}};
    String[][] singleMeasurement = {
      {"s0", "s1", "s2", "s3", "s4", "z0", "z1", "z2", "z3"},
      {}
    };
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /**
   * start test
   *
   * @param devices name and number of device
   * @param vectorMeasurement the number of device and the number of values to include in the tablet
   * @param singleMeasurement non-vector measurement name, set null if no need
   */
  private void test(String[] devices, int[][] vectorMeasurement, String[][] singleMeasurement) {
    // 1. generate file
    generateFile(devices, vectorMeasurement, singleMeasurement);
    // 2. read metadata from file
    List<String> actualDevices = new ArrayList<>(); // contains all device by sequence
    List<List<String>> actualMeasurements =
        new ArrayList<>(); // contains all measurements group by device
    readMetaDataDFS(actualDevices, actualMeasurements);
    // 3. generate correct result
    List<String> correctDevices = new ArrayList<>(); // contains all device by sequence
    List<List<String>> correctFirstMeasurements =
        new ArrayList<>(); // contains first measurements of every leaf, group by device
    List<String> correctPaths = new ArrayList<>(); // contains all paths by sequence
    generateCorrectResult(
        correctDevices,
        correctFirstMeasurements,
        correctPaths,
        devices,
        vectorMeasurement,
        singleMeasurement);
    // 4. compare correct result with TsFile's metadata
    Arrays.sort(devices);
    // 4.1 make sure device in order
    assertEquals(correctDevices.size(), devices.length);
    assertEquals(actualDevices.size(), correctDevices.size());
    for (int i = 0; i < actualDevices.size(); i++) {
      assertEquals(actualDevices.get(i), correctDevices.get(i));
    }
    // 4.2 make sure timeseries in order
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      Iterator<Pair<String, Boolean>> iterator = reader.getAllDevicesIteratorWithIsAligned();
      while (iterator.hasNext()) {
        for (String correctDevice : correctDevices) {
          assertEquals(correctDevice, iterator.next().left);
        }
      }
      assertFalse(iterator.hasNext());

      Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata(false);
      for (int j = 0; j < actualDevices.size(); j++) {
        for (int i = 0; i < actualMeasurements.get(j).size(); i++) {
          assertEquals(
              allTimeseriesMetadata.get(actualDevices.get(j)).get(i).getMeasurementId(),
              correctFirstMeasurements.get(j).get(i));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // 4.3 make sure split leaf correctly
    for (int j = 0; j < actualDevices.size(); j++) {
      for (int i = 0; i < actualMeasurements.get(j).size(); i++) {
        assertEquals(
            actualMeasurements.get(j).get(i),
            correctFirstMeasurements.get(j).get(i * conf.getMaxDegreeOfIndexNode()));
      }
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      Iterator<List<Path>> iterator = reader.getPathsIterator();
      int idx = 0;
      while (iterator.hasNext()) {
        for (Path actualPath : iterator.next()) {
          assertEquals(actualPath.getFullPath(), correctPaths.get(idx));
          idx++;
        }
      }
      assertEquals(correctPaths.size(), idx);
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * read TsFile metadata, load actual message in devices and measurements
   *
   * @param devices load actual devices
   * @param measurements load actual measurement(first of every leaf)
   */
  private void readMetaDataDFS(List<String> devices, List<List<String>> measurements) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      TsFileMetadata tsFileMetaData = reader.readFileMetadata();
      MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
      deviceDFS(devices, measurements, reader, metadataIndexNode);
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** DFS in device level load actual devices */
  private void deviceDFS(
      List<String> devices,
      List<List<String>> measurements,
      TsFileSequenceReader reader,
      MetadataIndexNode node) {
    try {
      assertTrue(
          node.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)
              || node.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE));
      for (int i = 0; i < node.getChildren().size(); i++) {
        MetadataIndexEntry metadataIndexEntry = node.getChildren().get(i);
        long endOffset = node.getEndOffset();
        if (i != node.getChildren().size() - 1) {
          endOffset = node.getChildren().get(i + 1).getOffset();
        }
        MetadataIndexNode subNode =
            reader.getMetadataIndexNode(metadataIndexEntry.getOffset(), endOffset);
        if (node.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
          devices.add(metadataIndexEntry.getName());
          measurements.add(new ArrayList<>());
          measurementDFS(devices.size() - 1, measurements, reader, subNode);
        } else if (node.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE)) {
          deviceDFS(devices, measurements, reader, subNode);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
  /** DFS in measurement level load actual measurements */
  private void measurementDFS(
      int deviceIndex,
      List<List<String>> measurements,
      TsFileSequenceReader reader,
      MetadataIndexNode node) {

    try {
      assertTrue(
          node.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)
              || node.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT));
      for (int i = 0; i < node.getChildren().size(); i++) {
        MetadataIndexEntry metadataIndexEntry = node.getChildren().get(i);
        long endOffset = node.getEndOffset();
        if (i != node.getChildren().size() - 1) {
          endOffset = node.getChildren().get(i + 1).getOffset();
        }
        if (node.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          measurements.get(deviceIndex).add(metadataIndexEntry.getName());
        } else if (node.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT)) {
          MetadataIndexNode subNode =
              reader.getMetadataIndexNode(metadataIndexEntry.getOffset(), endOffset);
          measurementDFS(deviceIndex, measurements, reader, subNode);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * generate correct devices and measurements for test Note that if the metadata index tree is
   * re-designed, you may need to modify this function as well.
   *
   * @param correctDevices output
   * @param correctMeasurements output
   * @param devices input
   * @param vectorMeasurement input
   * @param singleMeasurement input
   */
  private void generateCorrectResult(
      List<String> correctDevices,
      List<List<String>> correctMeasurements,
      List<String> correctPaths,
      String[] devices,
      int[][] vectorMeasurement,
      String[][] singleMeasurement) {
    for (int i = 0; i < devices.length; i++) {
      String device = devices[i];
      correctDevices.add(device);
      // generate measurement and sort
      List<String> measurements = new ArrayList<>();
      // single-variable measurement
      if (singleMeasurement != null) {
        for (String measurement : singleMeasurement[i]) {
          measurements.add(measurement);
          correctPaths.add(new Path(device, measurement, true).getFullPath());
        }
      }
      // multi-variable measurement
      for (int vectorIndex = 0; vectorIndex < vectorMeasurement[i].length; vectorIndex++) {
        measurements.add("");
        correctPaths.add(new Path(device, "", true).getFullPath());
        int measurementNum = vectorMeasurement[i][vectorIndex];
        for (int measurementIndex = 0; measurementIndex < measurementNum; measurementIndex++) {
          String measurementName =
              measurementPrefix + generateIndexString(measurementIndex, measurementNum);
          measurements.add(TsFileConstant.PATH_SEPARATOR + measurementName);
          correctPaths.add(new Path(device, measurementName, true).getFullPath());
        }
      }
      Collections.sort(measurements);
      correctMeasurements.add(measurements);
    }
    Collections.sort(correctDevices);
  }

  /**
   * @param devices name and number of device
   * @param vectorMeasurement the number of device and the number of values to include in the tablet
   * @param singleMeasurement non-vector measurement name, set null if no need
   */
  private void generateFile(
      String[] devices, int[][] vectorMeasurement, String[][] singleMeasurement) {
    File f = FSFactoryProducer.getFSFactory().getFile(FILE_PATH);
    if (f.exists() && !f.delete()) {
      fail("can not delete " + f.getAbsolutePath());
    }
    Schema schema = new Schema();
    try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {
      // write single-variable timeseries
      if (singleMeasurement != null) {
        for (int i = 0; i < singleMeasurement.length; i++) {
          String device = devices[i];
          for (String measurement : singleMeasurement[i]) {
            tsFileWriter.registerTimeseries(
                new Path(device),
                new MeasurementSchema(measurement, TSDataType.INT64, TSEncoding.RLE));
          }
          // the number of record rows
          int rowNum = 10;
          for (int row = 0; row < rowNum; row++) {
            TSRecord tsRecord = new TSRecord(row, device);
            for (String measurement : singleMeasurement[i]) {
              DataPoint dPoint = new LongDataPoint(measurement, row);
              tsRecord.addTuple(dPoint);
            }
            if (tsRecord.dataPointList.size() > 0) {
              tsFileWriter.write(tsRecord);
            }
          }
        }
      }

      // write multi-variable timeseries
      for (int i = 0; i < devices.length; i++) {
        String device = devices[i];
        logger.info("generating device {}...", device);
        // the number of rows to include in the tablet
        int rowNum = 10;
        for (int vectorIndex = 0; vectorIndex < vectorMeasurement[i].length; vectorIndex++) {
          String vectorName =
              vectorPrefix + generateIndexString(vectorIndex, vectorMeasurement.length);
          logger.info("generating vector {}...", vectorName);
          int measurementNum = vectorMeasurement[i][vectorIndex];
          List<MeasurementSchema> schemas = new ArrayList<>();
          List<MeasurementSchema> tabletSchema = new ArrayList<>();
          for (int measurementIndex = 0; measurementIndex < measurementNum; measurementIndex++) {
            String measurementName =
                measurementPrefix + generateIndexString(measurementIndex, measurementNum);
            logger.info("generating vector measurement {}...", measurementName);
            // add measurements into file schema (all with INT64 data type)
            MeasurementSchema schema1 =
                new MeasurementSchema(measurementName, TSDataType.INT64, TSEncoding.RLE);
            schemas.add(schema1);
            tabletSchema.add(schema1);
          }
          MeasurementGroup group = new MeasurementGroup(true, schemas);
          schema.registerMeasurementGroup(new Path(device), group);
          // add measurements into TSFileWriter
          // construct the tablet
          Tablet tablet = new Tablet(device, tabletSchema);
          long[] timestamps = tablet.timestamps;
          Object[] values = tablet.values;
          long timestamp = 1;
          long value = 1000000L;
          for (int r = 0; r < rowNum; r++, value++) {
            int row = tablet.rowSize++;
            timestamps[row] = timestamp++;
            for (int j = 0; j < measurementNum; j++) {
              long[] sensor = (long[]) values[j];
              sensor[row] = value;
            }
            // write Tablet to TsFile
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
              tsFileWriter.writeAligned(tablet);
              tablet.reset();
            }
          }
          // write Tablet to TsFile
          if (tablet.rowSize != 0) {
            tsFileWriter.writeAligned(tablet);
            tablet.reset();
          }
        }
      }
    } catch (Exception e) {
      logger.error("meet error in TsFileWrite with tablet", e);
      fail(e.getMessage());
    }
  }
}
