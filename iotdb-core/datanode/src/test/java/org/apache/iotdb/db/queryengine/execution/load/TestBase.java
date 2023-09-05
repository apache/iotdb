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

package org.apache.iotdb.db.queryengine.execution.load;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TestBase.class);
  public static final String BASE_OUTPUT_PATH = "target".concat(File.separator);
  public static final String PARTIAL_PATH_STRING =
      "%s" + File.separator + "%d" + File.separator + "%d" + File.separator;
  public static final String TEST_TSFILE_PATH =
      BASE_OUTPUT_PATH + "testTsFile".concat(File.separator) + PARTIAL_PATH_STRING;

  protected int fileNum = 100;
  // series number of each file, sn non-aligned series and 1 aligned series with sn measurements
  protected int seriesNum = 100;
  // number of chunks of each series in a file, each series has only one chunk in a file
  protected double chunkTimeRangeRatio = 0.3;
  // the interval between two consecutive points of a series
  protected long pointInterval = 50_000;
  protected List<File> files = new ArrayList<>();

  @Before
  public void setup() throws IOException, WriteProcessException {
    setupFiles();
    logger.info("Files set up");
  }

  @After
  public void cleanup() {
    for (File file : files) {
      file.delete();
    }
  }

  public void setupFiles() {

    IntStream.range(0, fileNum).parallel().forEach(i -> {
      try {
        File file = new File(getTestTsFilePath("root.sg1", 0, 0, i));
        synchronized (files) {
          files.add(file);
        }

        try (TsFileWriter writer = new TsFileWriter(file)) {
          // 3 non-aligned series under d1 and 1 aligned series with 3 measurements under d2
          for (int sn = 0; sn < seriesNum; sn++) {
            writer.registerTimeseries(
                new Path("d1"), new MeasurementSchema("s" + sn, TSDataType.DOUBLE));
          }
          List<MeasurementSchema> alignedSchemas = new ArrayList<>();
          for (int sn = 0; sn < seriesNum; sn++) {
            alignedSchemas.add(new MeasurementSchema("s" + sn, TSDataType.DOUBLE));
          }
          writer.registerAlignedTimeseries(new Path("d2"), alignedSchemas);

          long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
          long chunkTimeRange = (long) (timePartitionInterval * chunkTimeRangeRatio);
          int chunkPointNum = (int) (chunkTimeRange / pointInterval);

          for (int pn = 0; pn < chunkPointNum; pn++) {
            long currTime = chunkTimeRange * i + pointInterval * pn;
            TSRecord record = new TSRecord(currTime, "d1");
            for (int sn = 0; sn < seriesNum; sn++) {
              record.addTuple(new DoubleDataPoint("s" + sn, pn * 1.0));
            }
            writer.write(record);

            record.deviceId = "d2";
            writer.writeAligned(record);
          }
          writer.flushAllChunkGroups();
        }
      } catch (IOException | WriteProcessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static String getTestTsFilePath(
      String logicalStorageGroupName,
      long VirtualStorageGroupId,
      long TimePartitionId,
      long tsFileVersion) {
    String filePath =
        String.format(
            TEST_TSFILE_PATH, logicalStorageGroupName, VirtualStorageGroupId, TimePartitionId);
    return TsFileGeneratorUtils.getTsFilePath(filePath, tsFileVersion);
  }
}
