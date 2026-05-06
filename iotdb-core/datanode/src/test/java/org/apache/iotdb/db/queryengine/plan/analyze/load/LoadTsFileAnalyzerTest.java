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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class LoadTsFileAnalyzerTest {

  @Test
  public void testTableWritePointCountFallbackToTimeChunkWhenAllFieldsNull() throws Exception {
    final File tsFile = new File("load-table-all-null.tsfile");
    writeTableTsFileWithAllNullFields(tsFile);

    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final IDeviceID deviceID = new StringArrayDeviceID(new String[] {"table1", "tagA"});
      final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, false);
      Assert.assertEquals(1, alignedChunkMetadataList.size());

      final AbstractAlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
      Assert.assertNotNull(alignedChunkMetadata.getTimeChunkMetadata());
      Assert.assertEquals(
          3, alignedChunkMetadata.getTimeChunkMetadata().getStatistics().getCount());
      Assert.assertEquals(
          0,
          alignedChunkMetadata.getValueChunkMetadataList().stream()
              .filter(Objects::nonNull)
              .count());

      final Method method =
          LoadTsFileAnalyzer.class.getDeclaredMethod(
              "getTableWritePointCount", AbstractAlignedChunkMetadata.class);
      method.setAccessible(true);
      Assert.assertEquals(3L, method.invoke(null, alignedChunkMetadata));
    } finally {
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }
  }

  private void writeTableTsFileWithAllNullFields(final File tsFile) throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.DOUBLE));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(schema);
      writer.startChunkGroup(new StringArrayDeviceID(new String[] {"table1", "tagA"}));

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(tableSchemaList.subList(1, tableSchemaList.size()));
      for (int i = 0; i < 3; i++) {
        final long time = 100 + i;
        chunkWriter.getTimeChunkWriter().write(time);
        chunkWriter.getValueChunkWriterByIndex(0).write(time, 0L, true);
        chunkWriter.getValueChunkWriterByIndex(1).write(time, 0.0, true);
      }
      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
  }
}
