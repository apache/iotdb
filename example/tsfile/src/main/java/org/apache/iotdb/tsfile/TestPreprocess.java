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
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestPreprocess {

  public static final String deviceId = "root.SC.JZG.00";
  public static final String measurementId = "BHZ";

  public static void main(String[] args) {

    try {
      File outputFile =
          new File(
              "/Users/samperson1997/git/iotdb/data/data/sequence/root.SC/0/0/"
                  + measurementId
                  + ".tsfile");
      TsFileWriter tsFileWriter = new TsFileWriter(outputFile);
      tsFileWriter.registerTimeseries(
          new Path(deviceId, measurementId),
          new UnaryMeasurementSchema(measurementId, TSDataType.INT32, TSEncoding.PLAIN));

      for (int fileId = 1; fileId <= 30; fileId++) {
        File file =
            new File(
                "/Users/samperson1997/git/iotdb/data/2017/SC/JZG/"
                    + measurementId
                    + ".D1/SC.JZG.00."
                    + measurementId
                    + ".D.2017."
                    + generateIndexString(fileId)
                    + ".txt");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String tempString;
        int idx = 0;
        long startTime = 0L;

        while ((tempString = reader.readLine()) != null) {
          tempString = tempString.trim();
          if (tempString.startsWith("TraceID")
              || tempString.startsWith("DATA")
              || tempString.startsWith("Record")
              || tempString.startsWith("RECORD")
              || tempString.startsWith("Start")) {
            continue;
          }
          if (tempString.startsWith("Segment")) {
            String dateStr = tempString.split(" ")[1];
            Date date = sdf.parse(dateStr.substring(0, 10) + " " + dateStr.substring(11, 19));
            startTime = date.getTime();
            idx = 0;
            continue;
          }
          String[] values = tempString.split(" ");
          for (String s : values) {
            if (s.equals("")) {
              continue;
            }
            long time = startTime + idx * 10;
            int value = Integer.parseInt(s);
            TSRecord tsRecord = new TSRecord(time, deviceId);
            DataPoint dPoint1 = new IntDataPoint(measurementId, value);
            tsRecord.addTuple(dPoint1);
            tsFileWriter.write(tsRecord);
            idx++;
          }
        }
        reader.close();
      }

      tsFileWriter.close();
    } catch (IOException | ParseException | WriteProcessException e) {
      e.printStackTrace();
    }
  }

  private static String generateIndexString(int curIndex) {
    StringBuilder res = new StringBuilder(String.valueOf(curIndex));
    String target = String.valueOf(100);
    while (res.length() < target.length()) {
      res.insert(0, "0");
    }
    return res.toString();
  }
}
