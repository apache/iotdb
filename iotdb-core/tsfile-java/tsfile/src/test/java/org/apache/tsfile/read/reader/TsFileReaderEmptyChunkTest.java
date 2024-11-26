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
package org.apache.tsfile.read.reader;

import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TsFileReaderEmptyChunkTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileReaderEmptyChunkTest.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH).delete();
  }

  @Test
  public void testReadEmptyChunk() throws IOException {
    TsFileSequenceReader tsFileSequenceReader = null;
    TableQueryExecutor tableQueryExecutor = null;
    final List<String> measurementNames = Arrays.asList("s1", "s2", "s3", "s4");
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH))) {
      final String tableName = "table";
      registerTableSchema(writer, tableName);
      generateDevice(writer, tableName, 1, 1, 10);
      writer.endFile();

      tsFileSequenceReader = new TsFileSequenceReader(FILE_PATH, true, true);
      tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(tsFileSequenceReader),
              new CachedChunkLoaderImpl(tsFileSequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader tsBlockReader =
          tableQueryExecutor.query(tableName, measurementNames, null, null, null);

      int nullValueCount = 0;

      while (tsBlockReader.hasNext()) {
        final TsBlock tsBlock = tsBlockReader.next();
        final TsBlock.TsBlockRowIterator iterator = tsBlock.getTsBlockRowIterator();
        while (iterator.hasNext()) {
          final Object[] row = iterator.next();
          for (Object o : row) {
            if (o == null) {
              nullValueCount++;
            }
          }
        }
      }

      Assert.assertEquals(10, nullValueCount);
    } catch (final Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      if (tsFileSequenceReader != null) {
        tsFileSequenceReader.close();
      }
    }
  }

  private void registerTableSchema(final TsFileIOWriter writer, final String tableName) {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                "id", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.INT64),
            new MeasurementSchema("s3", TSDataType.INT64),
            new MeasurementSchema("s4", TSDataType.INT64));
    final List<Tablet.ColumnCategory> columnCategories =
        Arrays.asList(
            Tablet.ColumnCategory.ID,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT);
    final TableSchema tableSchema = new TableSchema(tableName, schemas, columnCategories);
    writer.getSchema().registerTableSchema(tableSchema);
  }

  private void generateDevice(
      final TsFileIOWriter writer,
      final String tableName,
      final int deviceNum,
      final int minTime,
      final int maxTime)
      throws IOException {
    for (int i = 0; i < deviceNum; i++) {
      final IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, "d" + i});
      writer.startChunkGroup(deviceID);
      final List<String> measurementNames = Arrays.asList("s1", "s2", "s3", "s4");
      generateSimpleAlignedSeriesToCurrentDevice(
          writer,
          Arrays.asList("s1", "s2", "s3", "s4"),
          new TimeRange[] {new TimeRange(minTime, maxTime)},
          new Random().nextInt(measurementNames.size()));
      writer.endChunkGroup();
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(
      final TsFileIOWriter writer,
      final List<String> measurementNames,
      final TimeRange[] toGenerateChunkTimeRanges,
      final int emptyChunkIndex)
      throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(
          new MeasurementSchema(
              measurementName, TSDataType.INT64, TSEncoding.RLE, CompressionType.LZ4));
    }

    for (TimeRange toGenerateChunk : toGenerateChunkTimeRanges) {
      final AlignedChunkWriterImpl alignedChunkWriter =
          new AlignedChunkWriterImpl(measurementSchemas);
      for (long time = toGenerateChunk.getMin(); time <= toGenerateChunk.getMax(); time++) {
        alignedChunkWriter.getTimeChunkWriter().write(time);
        for (int i = 0; i < measurementNames.size(); i++) {
          if (i == emptyChunkIndex) {
            continue;
          }

          alignedChunkWriter.getValueChunkWriterByIndex(i).write(time, time, false);
        }
      }
      alignedChunkWriter.writeToFileWriter(writer);
      writer.writeEmptyValueChunk(
          measurementNames.get(emptyChunkIndex),
          CompressionType.LZ4,
          TSDataType.INT64,
          TSEncoding.PLAIN,
          new LongStatistics());
    }
  }
}
