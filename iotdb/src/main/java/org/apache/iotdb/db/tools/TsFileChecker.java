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
package org.apache.iotdb.db.tools;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.bufferwrite.RestorableTsFileIOWriter;
import org.apache.iotdb.db.tools.TsFileChecker.TsFileStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.SystemConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileChecker implements Callable<TsFileStatus> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileChecker.class);

  private String path;

  public TsFileChecker(String path) {
    this.path = path;
  }

  public TsFileStatus checkTsFile(String path) {
    if (path.endsWith(RestorableTsFileIOWriter.RESTORE_SUFFIX)) {
      return TsFileStatus.RESTORE;
    }

    LOGGER.info("Start to check tsfile {}.", path);
    TsFileSequenceReader reader = null;
    long lastPosition = Long.MAX_VALUE;
    boolean timeSeq = true;
    try {
      boolean complete = true;
      String restoreFilePath = path + RestorableTsFileIOWriter.RESTORE_SUFFIX;
      long tsFileLen = new File(path).length();
      try {
        lastPosition = readLastPositionFromRestoreFile(restoreFilePath);
        complete = false;
      } catch (IOException ex) {
        lastPosition = tsFileLen;
        complete = true;
      }

      if (lastPosition > tsFileLen) {
        return TsFileStatus.UNCLOSED_DAMAGED;
      }
      if (lastPosition <= TSFileConfig.MAGIC_STRING.length()) {
        return TsFileStatus.UNCLOSED_INTACT;
      }

      reader = new TsFileSequenceReader(path, complete);
      if (!complete) {
        reader.position(TSFileConfig.MAGIC_STRING.length());
      }
      reader.readHeadMagic();
      byte marker;
      Map<String, long[]> timeRangeMap = new HashMap<>();
      Map<String, long[]> measurementTimeRangeMap = new HashMap<>();
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        if (reader.position() > lastPosition) {
          return timeSeq ? TsFileStatus.UNCLOSED_INTACT : TsFileStatus.UNCLOSED_INTACT_TIME_UNSEQ;
        }

        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader();
            List<Long> timeList = new ArrayList<>();
            Decoder defaultTimeDecoder = Decoder.getDecoderByType(
                TSEncoding.valueOf(TSFileConfig.timeSeriesEncoder),
                TSDataType.INT64);
            Decoder valueDecoder = Decoder
                .getDecoderByType(header.getEncodingType(), header.getDataType());
            for (int j = 0; j < header.getNumOfPages(); j++) {
              PageHeader pageHeader = reader.readPageHeader(header.getDataType());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 = new PageReader(pageData, header.getDataType(), valueDecoder,
                  defaultTimeDecoder);
              while (reader1.hasNextBatch()) {
                BatchData batchData = reader1.nextBatch();
                while (batchData.hasNext()) {
                  long time = batchData.currentTime();
                  timeList.add(time);
                  batchData.currentValue();
                  batchData.next();
                }
              }
            }
            if (timeSeq) {
              timeSeq = isTimeSequential(timeList);
              if (timeList.size() > 0) {
                long startTime = timeList.get(0);
                long endTime = timeList.get(timeList.size() - 1);
                timeSeq = updateTimeRange(measurementTimeRangeMap, header.getMeasurementID(), startTime, endTime);
              }
            }
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
            String deviceId = chunkGroupFooter.getDeviceID();

            if (timeSeq) {
              for (Entry<String, long[]> entry : measurementTimeRangeMap.entrySet()) {
                String key = deviceId + SystemConstant.PATH_SEPARATOR + entry.getKey();
                long[] range = entry.getValue();
                timeSeq = updateTimeRange(timeRangeMap, key, range[0], range[1]);
                if (!timeSeq) {
                  break;
                }
              }
              measurementTimeRangeMap.clear();
            }
            if (reader.position() == lastPosition) {
              return timeSeq ? TsFileStatus.UNCLOSED_INTACT : TsFileStatus.UNCLOSED_INTACT_TIME_UNSEQ;
            }
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

      TsFileMetaData metaData = reader.readFileMetadata();
      List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
          .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
      for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
        deviceMetadata.getChunkGroupMetaDataList();
      }
      reader.readTailMagic();
      reader.close();

      return timeSeq ? TsFileStatus.INTACT : TsFileStatus.INTACT_TIME_UNSEQ;
    } catch (Exception ex) {
      System.out.println(ex);
      try {
        if (reader != null && reader.position() > lastPosition) {
          return TsFileStatus.UNCLOSED_DAMAGED;
        } else {
          return TsFileStatus.DAMAGED;
        }
      } catch (IOException e) {
        return TsFileStatus.DAMAGED;
      }
    }
  }

  private boolean updateTimeRange(Map<String, long[]> rangeMap, String key, long startTime, long endTime) {
    if (!rangeMap.containsKey(key)) {
      rangeMap.put(key, new long[]{startTime, endTime});
      return true;
    }

    long[] range = rangeMap.get(key);
    if (range[1] >= startTime) {
      return false;
    }
    range[1] = endTime;
    return true;
  }

  private boolean isTimeSequential(List<Long> timeList) {
    if (timeList == null || timeList.size() <= 1) {
      return true;
    }
    long preTime = timeList.get(0);
    for (int i = 1; i < timeList.size(); i++) {
      long time = timeList.get(i);
      if (preTime >= time) {
        return false;
      }
      preTime = time;
    }
    return true;
  }

  private long readLastPositionFromRestoreFile(String path) throws IOException {
    int tsfilePositionByteSize = RestorableTsFileIOWriter.getTsPositionByteSize();
    byte[] lastPostionBytes = new byte[tsfilePositionByteSize];
    RandomAccessFile randomAccessFile = null;
    randomAccessFile = new RandomAccessFile(path, "r");

    long fileLength = randomAccessFile.length();
    randomAccessFile.seek(fileLength - tsfilePositionByteSize);
    randomAccessFile.read(lastPostionBytes);
    long lastPosition = BytesUtils.bytesToLong(lastPostionBytes);
    randomAccessFile.close();
    return lastPosition;
  }

  public static List<File> getFileList(String dirPath) {
    List<File> res = new ArrayList<>();
    File dir = new File(dirPath);
    if (dir.isFile()) {
      res.add(dir);
      return res;
    }

    File[] files = dir.listFiles();
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          res.addAll(getFileList(files[i].getAbsolutePath()));
        } else {
          res.add(files[i]);
        }
      }

    }
    return res;
  }

  public static List<File> getLastDirList(String dirPath) {
    List<File> res = new ArrayList<>();
    File dir = new File(dirPath);
    if (dir.isFile()) {
      return Collections.emptyList();
    }

    File[] files = dir.listFiles();
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          res.addAll(getLastDirList(files[i].getAbsolutePath()));
        } else {
          res.add(dir);
          return res;
        }
      }

    }
    return res;
  }

  @Override
  public TsFileStatus call() throws Exception {
    TsFileStatus status = checkTsFile(path);
    LOGGER.info("TsFile {}: {}", path, status);
    return status;
  }

  enum TsFileStatus {
    INTACT,
    INTACT_TIME_UNSEQ,
    DAMAGED,
    UNCLOSED_INTACT,
    UNCLOSED_INTACT_TIME_UNSEQ,
    UNCLOSED_DAMAGED,
    RESTORE
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 1) {
      System.out.println("Please input root dir!");
      System.exit(1);
    }

    String root = args[0];

    List<File> fileList = TsFileChecker.getFileList(root);
    System.out.println("Num of file: " + fileList.size());
    List<Pair<String, Future<TsFileStatus>>> futureList = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(fileList.size());
    for (File file : fileList) {
      String path = file.getAbsolutePath();
      TsFileChecker checker = new TsFileChecker(path);
      futureList.add(new Pair<>(path, executorService.submit(checker)));
    }

    Map<TsFileStatus, List<String>> statusMap = new HashMap<>();
    for (TsFileStatus status : TsFileStatus.values()) {
      statusMap.put(status, new ArrayList<>());
    }
    for (Pair<String, Future<TsFileStatus>> pair : futureList) {
      String path = pair.left;
      TsFileStatus status = pair.right.get();
      statusMap.get(status).add(path);
    }
    executorService.shutdown();

    System.out.println("TsFile status:");
    for (Entry<TsFileStatus, List<String>> entry : statusMap.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue().size());
    }
  }
}
