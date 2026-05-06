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

package org.apache.iotdb.db.storageengine.load.util;

import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.load.ObjectFileCorruptedException;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil.ModsInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class LoadObjectFileUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadObjectFileUtil.class);

  private LoadObjectFileUtil() {
    // util class
  }

  /**
   * Prepare object files required by local load.
   *
   * <p>1. Scan object columns in the TsFile and extract relative paths. 2. Validate object files
   * exist in search root. 3. Link/copy object files to a temporary target directory when needed.
   *
   * @return object file directory prepared for this local load
   */
  public static File prepareObjectFilesForLocalLoad(final LoadSingleTsFileNode node)
      throws LoadFileException, IOException {
    if (!node.isTsFileContainsObjectColumn()) {
      return node.getObjectFileSearchRoot();
    }

    final File tsFile = node.getTsFileResource().getTsFile();
    final File objectTempBaseDir =
        new File(IoTDBDescriptor.getInstance().getConfig().getLoadObjectFileTempDir());
    final String targetDirName = UUID.randomUUID().toString();
    final File targetDir = new File(objectTempBaseDir, targetDirName);
    final File searchRoot =
        node.getObjectFileSearchRoot() != null ? node.getObjectFileSearchRoot() : targetDir;
    final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        loadModificationsFromTsFile(tsFile);

    if (!targetDir.exists() && !targetDir.mkdirs() && !targetDir.exists()) {
      throw new LoadFileException(
          "Failed to create object temp directory: " + targetDir.getAbsolutePath());
    }

    final Set<String> processedPaths = new HashSet<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final TsFileSequenceReaderTimeseriesMetadataIterator iterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 100);

      while (iterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> deviceMetadataMap = iterator.next();
        for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
            deviceMetadataMap.entrySet()) {
          final IDeviceID deviceID = entry.getKey();
          final List<TimeseriesMetadata> metadataList = entry.getValue();
          final List<Map<Integer, long[]>> alignedTimeBatchesByChunkIndex =
              collectAlignedTimeBatchesByChunkIndex(reader, metadataList);
          for (final TimeseriesMetadata tsMetadata : metadataList) {
            if (tsMetadata.getTsDataType() != TSDataType.OBJECT) {
              continue;
            }

            final ModsInfo modsInfo =
                buildMeasurementModsInfo(deviceID, tsMetadata.getMeasurementId(), modifications);
            int chunkIndex = 0;
            for (final IChunkMetadata chunkMetadata : tsMetadata.getChunkMetadataList()) {
              scanObjectChunkAndProcessFiles(
                  reader,
                  chunkMetadata,
                  processedPaths,
                  searchRoot,
                  targetDir,
                  alignedTimeBatchesByChunkIndex,
                  chunkIndex++,
                  modsInfo);
            }
          }
        }
      }
    }

    LOGGER.info(
        "Successfully scanned and verified {} unique object files for local load of TsFile {}",
        processedPaths.size(),
        tsFile.getName());
    return targetDir;
  }

  private static void scanObjectChunkAndProcessFiles(
      final TsFileSequenceReader reader,
      final IChunkMetadata chunkMetadata,
      final Set<String> processedPaths,
      final File searchRoot,
      final File targetDir,
      final List<Map<Integer, long[]>> alignedTimeBatchesByChunkIndex,
      final int chunkIndex,
      final ModsInfo modsInfo)
      throws IOException, LoadFileException {
    reader.position(chunkMetadata.getOffsetOfChunkHeader());
    final byte marker = reader.readMarker();
    final ChunkHeader chunkHeader = reader.readChunkHeader(marker);
    final boolean hasStatistic =
        (byte) (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER;
    final boolean isAlignedValueChunk = (chunkHeader.getChunkType() & 0x40) != 0;

    final Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    final Decoder timeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);

    int dataSize = chunkHeader.getDataSize();
    int pageIndex = 0;
    while (dataSize > 0) {
      final PageHeader pageHeader = reader.readPageHeader(chunkHeader.getDataType(), hasStatistic);
      final ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
      if (isAlignedValueChunk) {
        if (chunkIndex < alignedTimeBatchesByChunkIndex.size()) {
          final long[] times = alignedTimeBatchesByChunkIndex.get(chunkIndex).get(pageIndex);
          if (times != null && times.length > 0) {
            final TsPrimitiveType[] values =
                new ValuePageReader(pageHeader, pageData, chunkHeader.getDataType(), valueDecoder)
                    .nextValueBatch(times);
            final int len = Math.min(values.length, times.length);
            for (int i = 0; i < len; i++) {
              if (values[i] == null || ModsOperationUtil.isDelete(times[i], modsInfo)) {
                continue;
              }
              final Binary binary = values[i].getBinary();
              if (binary == null) {
                continue;
              }
              final Pair<Long, String> lengthAndPath =
                  ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(binary);
              final long expectedLength = lengthAndPath.getLeft();
              final String relativePath = lengthAndPath.getRight();
              if (processedPaths.add(relativePath)) {
                processSingleObjectFile(searchRoot, targetDir, relativePath, expectedLength);
              }
            }
          }
        }
      } else {
        final BatchData batchData =
            new PageReader(
                    pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder)
                .getAllSatisfiedPageData();
        while (batchData.hasCurrent()) {
          final Binary binary = batchData.getBinary();
          if (binary != null && !ModsOperationUtil.isDelete(batchData.currentTime(), modsInfo)) {
            final Pair<Long, String> lengthAndPath =
                ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(binary);
            final long expectedLength = lengthAndPath.getLeft();
            final String relativePath = lengthAndPath.getRight();
            if (processedPaths.add(relativePath)) {
              processSingleObjectFile(searchRoot, targetDir, relativePath, expectedLength);
            }
          }
          batchData.next();
        }
      }
      dataSize -= pageHeader.getSerializedPageSize();
      pageIndex++;
    }
  }

  private static List<Map<Integer, long[]>> collectAlignedTimeBatchesByChunkIndex(
      final TsFileSequenceReader reader, final List<TimeseriesMetadata> metadataList)
      throws IOException {
    final List<Map<Integer, long[]>> result = new ArrayList<>();
    for (final TimeseriesMetadata metadata : metadataList) {
      if (!"".equals(metadata.getMeasurementId())) {
        continue;
      }
      for (final IChunkMetadata chunkMetadata : metadata.getChunkMetadataList()) {
        result.add(collectAlignedTimeBatchForChunk(reader, chunkMetadata));
      }
      break;
    }
    return result;
  }

  private static PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>
      loadModificationsFromTsFile(final File tsFile) throws IOException {
    final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    for (final ModEntry modification : ModificationFile.readAllModifications(tsFile, true)) {
      modifications.append(modification.keyOfPatternTree(), modification);
    }
    return modifications;
  }

  private static ModsInfo buildMeasurementModsInfo(
      final IDeviceID deviceID,
      final String measurement,
      final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications) {
    if (modifications == null || modifications.isEmpty()) {
      return null;
    }

    final List<ModsInfo> modsInfos =
        ModsOperationUtil.initializeMeasurementMods(
            deviceID, Collections.singletonList(measurement), modifications);
    return modsInfos.isEmpty() ? null : modsInfos.get(0);
  }

  private static Map<Integer, long[]> collectAlignedTimeBatchForChunk(
      final TsFileSequenceReader reader, final IChunkMetadata chunkMetadata) throws IOException {
    final Map<Integer, long[]> timeBatchByPage = new HashMap<>();
    final Decoder timeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);

    reader.position(chunkMetadata.getOffsetOfChunkHeader());
    final byte marker = reader.readMarker();
    final ChunkHeader chunkHeader = reader.readChunkHeader(marker);
    final boolean hasStatistic =
        (byte) (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER;

    int pageIndex = 0;
    int dataSize = chunkHeader.getDataSize();
    while (dataSize > 0) {
      final PageHeader pageHeader = reader.readPageHeader(chunkHeader.getDataType(), hasStatistic);
      final ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
      final long[] times = new TimePageReader(pageHeader, pageData, timeDecoder).getNextTimeBatch();
      timeBatchByPage.put(pageIndex++, times);
      dataSize -= pageHeader.getSerializedPageSize();
    }
    return timeBatchByPage;
  }

  /**
   * Clean up prepared object file directory if it is under configured object temp base directory.
   */
  public static void cleanupPreparedObjectTempDir(final File preparedDir) {
    if (preparedDir == null || !preparedDir.exists()) {
      return;
    }

    try {
      FileUtils.deleteFileOrDirectory(preparedDir);
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to cleanup prepared object temp directory: {}", preparedDir.getAbsolutePath(), e);
    }
  }

  private static void processSingleObjectFile(
      final File searchRoot,
      final File targetDir,
      final String relativePath,
      final long expectedLength)
      throws LoadFileException, IOException {
    final File sourceFile = new File(searchRoot, relativePath);
    if (!sourceFile.exists()) {
      throw new ObjectFileCorruptedException(
          "Object file does not exist: " + sourceFile.getAbsolutePath());
    }
    if (expectedLength >= 0 && sourceFile.length() != expectedLength) {
      throw new ObjectFileCorruptedException(
          String.format(
              "Object file size mismatch, expected %d but got %d, file: %s",
              expectedLength, sourceFile.length(), sourceFile.getAbsolutePath()));
    }

    final File targetFile = new File(targetDir, relativePath);
    if (sourceFile.getCanonicalFile().equals(targetFile.getCanonicalFile())) {
      return;
    }

    final File parentDir = targetFile.getParentFile();
    if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
      throw new LoadFileException(
          "Failed to create parent directory for object file: " + targetFile.getAbsolutePath());
    }

    if (!targetFile.exists()) {
      final FileStore sourceStore = Files.getFileStore(sourceFile.toPath());
      final FileStore targetStore = Files.getFileStore(targetDir.toPath());
      final boolean isSameLogicalDisk = sourceStore.equals(targetStore);
      if (isSameLogicalDisk) {
        try {
          FileUtils.createHardLink(sourceFile, targetFile);
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create hard link for object file, fallback to copy: {}",
              sourceFile.getAbsolutePath(),
              e);
          Files.copy(sourceFile.toPath(), targetFile.toPath());
        }
      } else {
        Files.copy(sourceFile.toPath(), targetFile.toPath());
      }
    }
  }
}
