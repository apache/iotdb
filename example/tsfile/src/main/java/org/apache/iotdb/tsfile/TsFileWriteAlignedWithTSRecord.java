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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileWriteAlignedWithTSRecord {
  private static final Logger logger =
      LoggerFactory.getLogger(TsFileWriteAlignedWithTSRecord.class);

  public static void main(String[] args) throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile("alignedRecord.tsfile");
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

      // register timeseries
      tsFileWriter.registerAlignedTimeseries(new Path("root.sg.d1"), measurementSchemas);

      List<IMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeAligned(tsFileWriter, "root.sg.d1", writeMeasurementScheams, 1000000, 0, 0);
    } catch (WriteProcessException e) {
      logger.error("write TSRecord failed", e);
    }
  }

  private static void writeAligned(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
      long rowSize,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    for (long time = startTime; time < rowSize + startTime; time++) {
      // construct TsRecord
      TSRecord tsRecord = new TSRecord(time, deviceId);
      for (IMeasurementSchema schema : schemas) {
        DataPoint dPoint = new LongDataPoint(schema.getMeasurementId(), startValue++);
        tsRecord.addTuple(dPoint);
      }
      // write
      tsFileWriter.writeAligned(tsRecord);
    }
  }
}
