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

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileWriteApiTest {
  private final File f = FSFactoryProducer.getFSFactory().getFile("TsFileWriteTest.tsfile");
  private final String deviceId = "root.sg.d1";
  private final List<UnaryMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
  private final List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();

  @Before
  public void setUp() {
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }
  }

  @After
  public void end() {
    if (f.exists()) f.delete();
  }

  public void registerAlignedTimeseries(TsFileWriter tsFileWriter) throws WriteProcessException {
    alignedMeasurementSchemas.add(
        new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    alignedMeasurementSchemas.add(
        new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    alignedMeasurementSchemas.add(
        new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.PLAIN));
    alignedMeasurementSchemas.add(
        new UnaryMeasurementSchema("s4", TSDataType.INT64, TSEncoding.RLE));

    // register align timeseries
    tsFileWriter.registerAlignedTimeseries(new Path(deviceId), alignedMeasurementSchemas);
  }

  public void registerTimeseries(TsFileWriter tsFileWriter) {
    measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.PLAIN));

    // register nonAlign timeseries
    tsFileWriter.registerTimeseries(new Path(deviceId), measurementSchemas);
  }

  @Test
  public void writeWithTsRecord() {
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerTimeseries(tsFileWriter);

      List<UnaryMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorForTest.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 10000, 0, 0, false);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeMeasurementScheams.add(measurementSchemas.get(0));
      TsFileGeneratorForTest.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 10000, 10000, 100, false);

      // example 3 : late data
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      TsFileGeneratorForTest.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 10, 0, 100, false);
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void writeAlignedWithTsRecord() {
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<UnaryMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorForTest.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 100000, 0, 0, true);

      // example2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      TsFileGeneratorForTest.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 200000, 10000000, 500, true);

      // example3 : late data
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      TsFileGeneratorForTest.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 20, 10000, 50, true);
    } catch (WriteProcessException | IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void writeWithTablet() {
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerTimeseries(tsFileWriter);

      List<UnaryMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorForTest.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 0, 0, false);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorForTest.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 2000, 0, false);

      // example 3: late data
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorForTest.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 20, 0, false);
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void writeAlignedWithTablet() {
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<UnaryMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      TsFileGeneratorForTest.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 10, 0, 0, true);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorForTest.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 200000, 10, 0, true);

      // example 3
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      TsFileGeneratorForTest.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 10, 0, 0, true);

    } catch (WriteProcessException | IOException e) {
      e.printStackTrace();
    }
  }
}
