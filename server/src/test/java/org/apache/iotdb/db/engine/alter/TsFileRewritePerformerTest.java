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
package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileRewritePerformerTest {

  String path =
      "data"
          .concat(File.separator)
          .concat("data")
          .concat(File.separator)
          .concat("sequence")
          .concat(File.separator)
          .concat("root.alt1")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("1-0-0-0.tsfile");
  String targetPath;
  String storageGroupName = "root.alt1";
  String device = "root.alt1.device_0";
  String alignedDevice = "root.alt1.device_1";
  String sensorPrefix = "sensor_";
  // the number of rows to include in the tablet
  int rowNum = 1000000;
  // the number of values to include in the tablet
  int sensorNum = 10;

  TSEncoding defaultEncoding = TSEncoding.TS_2DIFF;
  CompressionType defaultCompressionType = CompressionType.GZIP;

  @Before
  public void setUp() throws Exception {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }

      Schema schema = new Schema();

      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema measurementSchema =
            new MeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, defaultEncoding, defaultCompressionType);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device),
            new MeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, defaultEncoding, defaultCompressionType));
      }
      // add aligned measurements into file schema
      List<MeasurementSchema> schemas = new ArrayList<>();
      List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema schema1 =
            new MeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, defaultEncoding, defaultCompressionType);
        schemas.add(schema1);
        alignedMeasurementSchemas.add(schema1);
      }
      MeasurementGroup group = new MeasurementGroup(true, schemas);
      schema.registerMeasurementGroup(new Path(alignedDevice), group);

      try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {

        // add measurements into TSFileWriter
        // construct the tablet
        Tablet tablet = new Tablet(device, measurementSchemas);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        long timestamp = 1;
        long value = 1000000L;
        for (int r = 0; r < rowNum; r++, value++) {
          int row = tablet.rowSize++;
          timestamps[row] = timestamp++;
          for (int i = 0; i < sensorNum; i++) {
            long[] sensor = (long[]) values[i];
            sensor[row] = value;
          }
          // write Tablet to TsFile
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.write(tablet);
            tablet.reset();
          }
        }
        // write Tablet to TsFile
        if (tablet.rowSize != 0) {
          tsFileWriter.write(tablet);
          tablet.reset();
        }

        // add aligned measurements into TSFileWriter
        // construct the tablet
        tablet = new Tablet(alignedDevice, alignedMeasurementSchemas);
        timestamps = tablet.timestamps;
        values = tablet.values;
        timestamp = 1;
        value = 1000000L;
        for (int r = 0; r < rowNum; r++, value++) {
          int row = tablet.rowSize++;
          timestamps[row] = timestamp++;
          for (int i = 0; i < sensorNum; i++) {
            long[] sensor = (long[]) values[i];
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
    } catch (Exception e) {
      throw new Exception("meet error in TsFileWrite with tablet", e);
    }
  }

  @Test
  public void tsFileRewriteExcutorAlignedTest() {

    Map<String, Map<String, Pair<TSEncoding, CompressionType>>> alters = new HashMap<>();

    Map<String, Pair<TSEncoding, CompressionType>> alginMap = new HashMap<>();
    alters.put(alignedDevice, alginMap);
    alginMap.put(sensorPrefix + "1", new Pair<>(TSEncoding.PLAIN, CompressionType.SNAPPY));

    Map<String, Pair<TSEncoding, CompressionType>> notalginMap = new HashMap<>();
    alters.put(device, notalginMap);
    notalginMap.put(sensorPrefix + "1", new Pair<>(TSEncoding.GORILLA, CompressionType.GZIP));

    File targetTsFile = null;
    TsFileRewritePerformer excutor = null;
    try {
      AlteringRecordsCache alteringRecordsCache = AlteringRecordsCache.getInstance();
      alters.forEach(
          (d, m) -> {
            m.forEach(
                (mid, ap) -> {
                  PartialPath fullPath = null;
                  try {
                    fullPath = new PartialPath(d, mid);
                    alteringRecordsCache.putRecord(
                        storageGroupName, fullPath.getFullPath(), ap.left, ap.right);
                  } catch (Exception e) {
                    Assert.fail(e.getMessage());
                  }
                });
          });
      alteringRecordsCache.startAlter();
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (!f.exists()) {
        throw new RuntimeException("tsfile not exists " + f.getAbsolutePath());
      }
      TsFileResource tsFileResource = new TsFileResource(f);
      tsFileResource.close();
      TsFileResource targetTsFileResource =
          TsFileNameGenerator.generateNewAlterTsFileResource(tsFileResource);
      excutor = new TsFileRewritePerformer();
      excutor.setSourceFiles(Collections.singletonList(tsFileResource));
      excutor.setTargetFiles(Collections.singletonList(targetTsFileResource));
      excutor.setSummary(new CompactionTaskSummary());
      excutor.perform();
      targetTsFile = targetTsFileResource.getTsFile();
      alteringRecordsCache.clear(storageGroupName);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    if (targetTsFile == null || !targetTsFile.exists()) {
      throw new RuntimeException("target not exists " + targetTsFile.getAbsolutePath());
    }
    targetPath = targetTsFile.getAbsolutePath();
    Assert.assertFalse(AlteringRecordsCache.getInstance().isAltering());
    readCheck(alters, targetTsFile);
  }

  private void readCheck(
      Map<String, Map<String, Pair<TSEncoding, CompressionType>>> alters, File targetTsFile) {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetTsFile.getAbsolutePath(), true)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();

      while (deviceIterator.hasNext()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.next();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;
        if (aligned) {
          List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
          for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
            List<IChunkMetadata> valueChunkMetadataList =
                alignedChunkMetadata.getValueChunkMetadataList();
            for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
              Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
              ChunkHeader header = chunk.getHeader();
              Map<String, Pair<TSEncoding, CompressionType>> deviceMap = alters.get(device);
              if (deviceMap != null && deviceMap.containsKey(header.getMeasurementID())) {
                Pair<TSEncoding, CompressionType> tsEncodingCompressionTypePair =
                    deviceMap.get(header.getMeasurementID());
                Assert.assertEquals(header.getEncodingType(), tsEncodingCompressionTypePair.left);
                Assert.assertEquals(
                    header.getCompressionType(), tsEncodingCompressionTypePair.right);
              } else {
                Assert.assertEquals(header.getEncodingType(), defaultEncoding);
                Assert.assertEquals(header.getCompressionType(), defaultCompressionType);
              }
            }
          }
        } else {
          Map<String, List<ChunkMetadata>> measurementMap =
              reader.readChunkMetadataInDevice(device);
          for (Map.Entry<String, List<ChunkMetadata>> next : measurementMap.entrySet()) {
            String measurementId = next.getKey();
            List<ChunkMetadata> chunkMetadatas = next.getValue();
            for (ChunkMetadata chunkMetadata : chunkMetadatas) {
              Chunk currentChunk = reader.readMemChunk(chunkMetadata);
              ChunkHeader header = currentChunk.getHeader();
              Map<String, Pair<TSEncoding, CompressionType>> deviceMap = alters.get(device);
              if (deviceMap != null && deviceMap.containsKey(measurementId)) {
                Pair<TSEncoding, CompressionType> tsEncodingCompressionTypePair =
                    deviceMap.get(header.getMeasurementID());
                Assert.assertEquals(header.getEncodingType(), tsEncodingCompressionTypePair.left);
                Assert.assertEquals(
                    header.getCompressionType(), tsEncodingCompressionTypePair.right);
              } else {
                Assert.assertEquals(header.getEncodingType(), defaultEncoding);
                Assert.assertEquals(header.getCompressionType(), defaultCompressionType);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      if (targetPath != null) {
        try {
          File file = new File(targetPath);
          if (file.exists()) {
            FileUtils.forceDelete(file);
          }
        } catch (IOException e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  @After
  public void tearDown() {
    try {
      FileUtils.forceDelete(new File(path));
      if (targetPath != null) {
        try {
          File file = new File(targetPath);
          if (file.exists()) {
            FileUtils.forceDelete(file);
          }
        } catch (IOException e) {
          Assert.fail(e.getMessage());
        }
      }
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  protected List<IMeasurementSchema> collectSchemaList(
      List<AlignedChunkMetadata> alignedChunkMetadatas, TsFileSequenceReader reader)
      throws IOException {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    Set<String> measurementSet = new HashSet<>();
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
        if (chunkMetadata == null) {
          continue;
        }
        String measurementId = chunkMetadata.getMeasurementUid();
        if (measurementSet.contains(measurementId)) {
          continue;
        }
        measurementSet.add(measurementId);
        Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
        ChunkHeader header = chunk.getHeader();
        MeasurementSchema measurementSchema =
            new MeasurementSchema(
                header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(),
                header.getCompressionType());
        schemaList.add(measurementSchema);
      }
    }

    schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return schemaList;
  }
}
