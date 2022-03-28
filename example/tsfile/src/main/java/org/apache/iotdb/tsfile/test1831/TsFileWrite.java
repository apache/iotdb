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

import org.apache.iotdb.tsfile.Constant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.io.File;
import java.util.Random;

/**
 * An example of writing data with TSRecord to TsFile It uses the interface: public void
 * addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
 */
public class TsFileWrite {

  public static int chunkNum;
  public static int fileNum = 500;

  public static void main(String[] args) {
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
      try {
        String path =
            "/home/fit/szs/data/data/sequence/root.sg/0/"
                + chunkNum
                + "/test"
                + fileIndex
                + ".tsfile";
        File f = FSFactoryProducer.getFSFactory().getFile(path);
        if (f.exists()) {
          f.delete();
        }

        try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
          // 1000 timeseries
          for (int i = 1; i <= 1000; i++) {
            tsFileWriter.registerTimeseries(
                new Path(Constant.DEVICE_PREFIX, Constant.SENSOR_ + i),
                new UnaryMeasurementSchema(Constant.SENSOR_ + i, TSDataType.INT64, TSEncoding.RLE));
          }
          // construct TSRecord
          for (int i = 1; i <= chunkNum * 100; i++) {
            TSRecord tsRecord = new TSRecord(i, Constant.DEVICE_PREFIX);
            for (int t = 1; t <= 1000; t++) {
              DataPoint dPoint1 = new LongDataPoint(Constant.SENSOR_ + t, new Random().nextLong());
              tsRecord.addTuple(dPoint1);
            }
            // write TSRecord
            tsFileWriter.write(tsRecord);
            if (i % 100 == 0) {
              tsFileWriter.flushAllChunkGroups();
            }
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
        System.out.println(e.getMessage());
      }
    }
  }
}
