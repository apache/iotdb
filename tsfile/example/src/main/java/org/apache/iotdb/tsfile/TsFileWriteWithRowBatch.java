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

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.record.RowBatch;
/**
 * An example of writing data with RowBatch to TsFile
 */
public class TsFileWriteWithRowBatch {

  public static void main(String[] args) {
    try {
      String path = "test.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }

      FileSchema fileSchema = new FileSchema();

      // the number of rows to include in the row batch
      int rowNum = 1000000;
      // the number of values to include in the row batch
      int sensorNum = 10;

      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        fileSchema.registerMeasurement(
                new MeasurementSchema("sensor_" + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
      }

      // add measurements into TSFileWriter
      TsFileWriter tsFileWriter = new TsFileWriter(f, fileSchema);

      // construct the row batch
      RowBatch rowBatch = fileSchema.createRowBatch("device_1");

      long[] timestamps = rowBatch.timestamps;
      Object[] values = rowBatch.values;

      long timestamp = 1;
      long value = 1000000L;

      for (int r = 0; r < rowNum; r++, value++) {
        int row = rowBatch.batchSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < sensorNum; i++) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        // write RowBatch to TsFile
        if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
          tsFileWriter.write(rowBatch);
          rowBatch.reset();
        }
      }
      // write RowBatch to TsFile
      if (rowBatch.batchSize != 0) {
        tsFileWriter.write(rowBatch);
        rowBatch.reset();
      }

      // close TsFile
      tsFileWriter.close();
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
