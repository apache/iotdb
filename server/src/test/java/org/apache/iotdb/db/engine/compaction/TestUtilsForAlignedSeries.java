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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class TestUtilsForAlignedSeries {
  public static void registerTimeSeries(
      String storageGroup, String[] devices, IMeasurementSchema[] schemas, boolean[] isAligned)
      throws MetadataException {}

  public static void writeTsFile(
      String[] devices,
      IMeasurementSchema[] schemas,
      TsFileResource tsFileResource,
      boolean[] alignedArray,
      long startTime,
      long endTime,
      boolean[] randomNull)
      throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(tsFileResource.getTsFile())) {
      for (int i = 0; i < devices.length; ++i) {
        String device = devices[i];
        boolean aligned = alignedArray[i];
        if (aligned) {
          writeAlignedChunkGroup(writer, device, schemas, startTime, endTime, randomNull[i]);
        } else {
          writeNotAlignedChunkGroup(writer, device, schemas, startTime, endTime, randomNull[i]);
        }
        tsFileResource.updateStartTime(devices[i], startTime);
        tsFileResource.updateEndTime(devices[i], endTime);
      }
      writer.endFile();
    }
    tsFileResource.close();
    tsFileResource.serialize();
  }

  private static void writeAlignedChunkGroup(
      TsFileIOWriter writer,
      String device,
      IMeasurementSchema[] schemas,
      long startTime,
      long endTime,
      boolean randomNull)
      throws IOException {
    writer.startChunkGroup(device);
    AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(Arrays.asList(schemas));
    Random random = new Random();
    for (long time = startTime; time < endTime; ++time) {
      for (int i = 0; i < schemas.length; ++i) {
        switch (schemas[i].getType()) {
          case BOOLEAN:
            boolean booleanVal = (boolean) generateRandomVal(schemas[i].getType());
            alignedChunkWriter.write(time, booleanVal, randomNull && random.nextInt(2) == 1);
            break;
          case INT32:
            int intVal = (int) generateRandomVal(schemas[i].getType());
            alignedChunkWriter.write(time, intVal, randomNull && random.nextInt(2) == 1);
            break;
          case DOUBLE:
            double doubleVal = (double) generateRandomVal(schemas[i].getType());
            alignedChunkWriter.write(time, doubleVal, randomNull && random.nextInt(2) == 1);
            break;
          case FLOAT:
            float floatVal = (float) generateRandomVal(schemas[i].getType());
            alignedChunkWriter.write(time, floatVal, randomNull && random.nextInt(2) == 1);
            break;
          case TEXT:
            String stringVal = (String) generateRandomVal(schemas[i].getType());
            alignedChunkWriter.write(
                time,
                new Binary(stringVal.getBytes(StandardCharsets.UTF_8)),
                randomNull && random.nextInt(2) == 1);
            break;
          case INT64:
            long longVal = (long) generateRandomVal(schemas[i].getType());
            alignedChunkWriter.write(time, longVal, randomNull && random.nextInt(2) == 1);
            break;
        }
      }
      alignedChunkWriter.write(time);
    }
    alignedChunkWriter.writeToFileWriter(writer);
    writer.endChunkGroup();
  }

  private static void writeNotAlignedChunkGroup(
      TsFileIOWriter writer,
      String device,
      IMeasurementSchema[] schemas,
      long startTime,
      long endTime,
      boolean randomNull)
      throws IOException {
    writer.startChunkGroup(device);
    Random random = new Random();
    for (IMeasurementSchema schema : schemas) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      for (long time = startTime; time < endTime; ++time) {
        if (randomNull && random.nextInt(2) == 1) {
          continue;
        }
        switch (schema.getType()) {
          case BOOLEAN:
            boolean booleanVal = (boolean) generateRandomVal(schema.getType());
            chunkWriter.write(time, booleanVal);
            break;
          case INT32:
            int intVal = (int) generateRandomVal(schema.getType());
            chunkWriter.write(time, intVal);
            break;
          case DOUBLE:
            double doubleVal = (double) generateRandomVal(schema.getType());
            chunkWriter.write(time, doubleVal);
            break;
          case FLOAT:
            float floatVal = (float) generateRandomVal(schema.getType());
            chunkWriter.write(time, floatVal);
            break;
          case TEXT:
            String stringVal = (String) generateRandomVal(schema.getType());
            chunkWriter.write(time, new Binary(stringVal.getBytes(StandardCharsets.UTF_8)));
            break;
          case INT64:
            long longVal = (long) generateRandomVal(schema.getType());
            chunkWriter.write(time, longVal);
            break;
        }
      }
      chunkWriter.writeToFileWriter(writer);
    }
    writer.endChunkGroup();
  }

  private static Object generateRandomVal(TSDataType type) {
    Random random = new Random();
    Object returnVal = null;
    switch (type) {
      case BOOLEAN:
        returnVal = random.nextInt(2) == 0;
        break;
      case INT32:
        returnVal = random.nextInt();
        break;
      case DOUBLE:
        returnVal = random.nextDouble();
        break;
      case FLOAT:
        returnVal = random.nextFloat();
        break;
      case TEXT:
        returnVal = String.valueOf(random.nextLong());
        break;
      case INT64:
        returnVal = random.nextLong();
        break;
    }
    return returnVal;
  }
}
