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
package org.apache.iotdb.db.tools.utils;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("java:S2175")
public class TsFileValidationScan extends TsFileSequenceScan {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileValidationScan.class);
  private static final String STR_FIND_BAD_FILE = "-- Find the bad file ";
  private static final String STR_OVERLAP_LATTER_FILE = ", overlap with latter files.";
  private static final String STR_OVERLAP_BETWEEN_FILE =
      " overlap between files, with previous file ";
  private static final String STR_TIMESERIES = "-------- Timeseries ";
  private static final String FILE_NON_EXIST = "";
  private static final IDeviceID EMPTY_DEVICE_ID = new PlainDeviceID("");

  protected boolean printDetails;
  protected boolean ignoreFileOverlap = false;

  // chunk/page context
  protected long currentChunkEndTime;
  protected long currentPageEndTime;
  protected long lastPageEndTime;

  // single file context
  protected TsFileResource resource;
  // deviceID -> has checked overlap or not
  protected Map<IDeviceID, Boolean> hasCheckedDeviceOverlap;
  protected Map<Pair<IDeviceID, String>, SeriesOverlapPrintInfo> hasSeriesPrintedDetails;
  // measurementId -> lastChunkEndTime in current file
  protected Map<Pair<IDeviceID, String>, Long> lashChunkEndTime;

  // global context
  // measurementID -> <fileName, [lastTime, endTimeInLastFile]>
  protected Map<Pair<IDeviceID, String>, FileLastTimeInfo> timeseriesLastTimeMap = new HashMap<>();
  // deviceID -> <fileName, endTime>, the endTime of device in the last seq file
  protected Map<IDeviceID, FileLastTimeInfo> deviceEndTime = new HashMap<>();
  // fileName -> isBadFile
  protected Map<String, Boolean> isBadFileMap = new HashMap<>();
  protected int badFileNum;
  protected List<String> previousBadFileMsgs = new ArrayList<>();

  public TsFileValidationScan() {
    super();
  }

  @Override
  protected boolean onFileOpen(File file) throws IOException {
    super.onFileOpen(file);

    hasCheckedDeviceOverlap = new HashMap<>();
    currDeviceID = EMPTY_DEVICE_ID;
    hasSeriesPrintedDetails = new HashMap<>();
    lashChunkEndTime = new HashMap<>();

    resource = new TsFileResource(file);
    if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()) {
      // resource file does not exist, tsfile may not be flushed yet
      LOGGER.warn(
          "{} does not exist ,skip it.", file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
      return false;
    } else {
      resource.deserialize();
    }
    isBadFileMap.put(file.getName(), false);
    return true;
  }

  @Override
  protected void onFileEnd() {
    // record the end time of each timeseries in current file
    for (Map.Entry<Pair<IDeviceID, String>, Long> entry : lashChunkEndTime.entrySet()) {
      FileLastTimeInfo fileNameLastTime =
          timeseriesLastTimeMap.computeIfAbsent(currTimeseriesID, id -> new FileLastTimeInfo());
      if (fileNameLastTime.endTimeInLastFile <= entry.getValue()) {
        fileNameLastTime.endTimeInLastFile = entry.getValue();
        fileNameLastTime.lastFileName = file.getName();
      }
    }
    if (ignoreFileOverlap) {
      reset(false);
    }
  }

  @Override
  protected void onChunkGroup() throws IOException {
    FileLastTimeInfo fileNameLastTimePair =
        deviceEndTime.computeIfAbsent(currDeviceID, k -> new FileLastTimeInfo());
    if (!currDeviceID.equals(EMPTY_DEVICE_ID)) {
      long endTime = resource.getEndTime(currDeviceID);
      // record the end time of last device in current file
      if (endTime > fileNameLastTimePair.lastTime) {
        fileNameLastTimePair.lastFileName = file.getName();
        fileNameLastTimePair.endTimeInLastFile = endTime;
      }
    }

    super.onChunkGroup();

    fileNameLastTimePair = deviceEndTime.computeIfAbsent(currDeviceID, k -> new FileLastTimeInfo());
    if (!Boolean.TRUE.equals(hasCheckedDeviceOverlap.getOrDefault(currDeviceID, false))
        && resource.getStartTime(currDeviceID) <= fileNameLastTimePair.endTimeInLastFile) {
      // device overlap, find bad file
      recordDeviceOverlap(fileNameLastTimePair.lastFileName);
    }

    hasCheckedDeviceOverlap.put(currDeviceID, true);
  }

  private void recordDeviceOverlap(String badFileName) {
    // add previous bad file msg to list
    if (!Boolean.TRUE.equals(isBadFileMap.get(badFileName))) {
      if (printDetails) {
        previousBadFileMsgs.add(
            STR_FIND_BAD_FILE
                + file.getParentFile().getAbsolutePath()
                + File.separator
                + deviceEndTime.get(currDeviceID).lastFileName
                + STR_OVERLAP_LATTER_FILE);
      } else {
        previousBadFileMsgs.add(
            file.getParentFile().getAbsolutePath()
                + File.separator
                + deviceEndTime.get(currDeviceID).lastFileName);
      }
      isBadFileMap.put(badFileName, true);
      badFileNum++;
    }
    // print current file
    if (!Boolean.TRUE.equals(isBadFileMap.get(file.getName()))) {
      if (printDetails) {
        printBoth(STR_FIND_BAD_FILE + file.getAbsolutePath());
      } else {
        printBoth(file.getAbsolutePath());
      }
      isBadFileMap.put(file.getName(), true);
      badFileNum++;
    }
    if (printDetails) {
      printBoth(
          "---- Device "
              + currDeviceID
              + STR_OVERLAP_BETWEEN_FILE
              + deviceEndTime.get(currDeviceID).lastFileName);
    }
  }

  @Override
  protected void onTimePage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader)
      throws IOException {
    // Time Chunk
    Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
    long[] timeBatch = timePageReader.getNextTimeBatch();
    FileLastTimeInfo fileNameLastTimePair =
        timeseriesLastTimeMap.computeIfAbsent(currTimeseriesID, id -> new FileLastTimeInfo());
    for (long timestamp : timeBatch) {
      onTimeStamp(timestamp, fileNameLastTimePair);
    }
    lastPageEndTime = Math.max(lastPageEndTime, currentPageEndTime);
  }

  protected void markInFileOverlap() {
    if (!Boolean.TRUE.equals(isBadFileMap.get(file.getName()))) {
      if (printDetails) {
        printBoth(STR_FIND_BAD_FILE + file.getAbsolutePath());
      } else {
        printBoth(file.getAbsolutePath());
      }
      isBadFileMap.put(file.getName(), true);
      badFileNum++;
    }
  }

  private void markBetweenFileOverlap(String overLapFile) {
    // overlap between file, then add previous bad file path to list
    if (!Boolean.TRUE.equals(isBadFileMap.getOrDefault(overLapFile, false))) {
      if (printDetails) {
        previousBadFileMsgs.add(
            STR_FIND_BAD_FILE
                + file.getParentFile().getAbsolutePath()
                + File.separator
                + overLapFile
                + STR_OVERLAP_LATTER_FILE);
      } else {
        previousBadFileMsgs.add(
            file.getParentFile().getAbsolutePath() + File.separator + overLapFile);
      }
      isBadFileMap.put(overLapFile, true);
      badFileNum++;
    }
  }

  protected void printOverlapDetails(long timestamp, FileLastTimeInfo seriesLastTime) {
    SeriesOverlapPrintInfo seriesOverlapPrintInfo =
        hasSeriesPrintedDetails.computeIfAbsent(
            currTimeseriesID, k -> new SeriesOverlapPrintInfo());
    if (timestamp <= seriesLastTime.endTimeInLastFile) {
      if (!seriesOverlapPrintInfo.betweenFileOverlapPrinted) {
        printBoth(
            STR_TIMESERIES
                + currTimeseriesID
                + STR_OVERLAP_BETWEEN_FILE
                + seriesLastTime.lastFileName);
        seriesOverlapPrintInfo.betweenFileOverlapPrinted = true;
      }
    } else if (timestamp <= lashChunkEndTime.getOrDefault(currTimeseriesID, Long.MIN_VALUE)) {
      if (!seriesOverlapPrintInfo.chunkOverlapPrinted) {
        printBoth(STR_TIMESERIES + currTimeseriesID + " overlap between chunks");
        seriesOverlapPrintInfo.chunkOverlapPrinted = true;
      }
    } else if (timestamp <= lastPageEndTime) {
      if (!seriesOverlapPrintInfo.crossPageOverlapPrinted) {
        printBoth(STR_TIMESERIES + currTimeseriesID + " overlap between pages");
        seriesOverlapPrintInfo.crossPageOverlapPrinted = true;
      }
    } else {
      if (!seriesOverlapPrintInfo.inPagePOverlapPrinted) {
        printBoth(STR_TIMESERIES + currTimeseriesID + " overlap within one page");
        seriesOverlapPrintInfo.inPagePOverlapPrinted = true;
      }
    }
  }

  @Override
  protected void onValuePage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) {
    // Value Page, skip it
  }

  @Override
  protected void onNonAlignedPage(
      PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) throws IOException {
    // NonAligned Chunk
    Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, defaultTimeDecoder);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    FileLastTimeInfo fileNameLastTimePair =
        timeseriesLastTimeMap.computeIfAbsent(currTimeseriesID, id -> new FileLastTimeInfo());
    while (batchData.hasCurrent()) {
      long timestamp = batchData.currentTime();
      onTimeStamp(timestamp, fileNameLastTimePair);
      batchData.next();
    }
    lastPageEndTime = Math.max(lastPageEndTime, currentPageEndTime);
  }

  private void onTimeStamp(long timestamp, FileLastTimeInfo fileNameLastTimePair) {
    if (timestamp <= fileNameLastTimePair.lastTime) {
      // find bad file
      markInFileOverlap();

      if (timestamp <= fileNameLastTimePair.endTimeInLastFile) {
        markBetweenFileOverlap(fileNameLastTimePair.lastFileName);
      }

      if (printDetails) {
        printOverlapDetails(timestamp, fileNameLastTimePair);
      }
    } else {
      fileNameLastTimePair.lastTime = timestamp;
      currentPageEndTime = timestamp;
      currentChunkEndTime = timestamp;
    }
  }

  @Override
  protected void onChunk(PageVisitor pageVisitor) throws IOException {
    currentChunkEndTime = Long.MIN_VALUE;
    lastPageEndTime = Long.MIN_VALUE;

    super.onChunk(pageVisitor);

    lashChunkEndTime.put(
        currTimeseriesID,
        Math.max(
            lashChunkEndTime.getOrDefault(currTimeseriesID, Long.MIN_VALUE), currentChunkEndTime));
  }

  public List<String> getPreviousBadFileMsgs() {
    return previousBadFileMsgs;
  }

  @Override
  protected void onException(Throwable t) {
    LOGGER.error("Meet errors in reading file {} , skip it.", file.getAbsolutePath(), t);
    if (!Boolean.TRUE.equals(isBadFileMap.get(file.getName()))) {
      if (printDetails) {
        printBoth(
            "-- Meet errors in reading file "
                + file.getAbsolutePath()
                + ", tsfile may be corrupted.");
      } else {
        printBoth(file.getAbsolutePath());
      }
      isBadFileMap.put(file.getName(), true);
      badFileNum++;
    }
  }

  public int getBadFileNum() {
    return badFileNum;
  }

  @TestOnly
  public void setBadFileNum(int badFileNum) {
    this.badFileNum = badFileNum;
  }

  public void reset(boolean resetBadFileNum) {
    if (resetBadFileNum) {
      badFileNum = 0;
    }
    timeseriesLastTimeMap.clear();
    deviceEndTime.clear();
    isBadFileMap.clear();
  }

  protected static class FileLastTimeInfo {

    public FileLastTimeInfo() {
      lastFileName = FILE_NON_EXIST;
      lastTime = Long.MIN_VALUE;
      endTimeInLastFile = Long.MIN_VALUE;
    }

    private String lastFileName;
    private long lastTime;
    private long endTimeInLastFile;
  }

  protected static class SeriesOverlapPrintInfo {

    private boolean betweenFileOverlapPrinted;
    private boolean chunkOverlapPrinted;
    private boolean crossPageOverlapPrinted;
    private boolean inPagePOverlapPrinted;
  }

  public void setPrintDetails(boolean printDetails) {
    this.printDetails = printDetails;
  }

  public void setIgnoreFileOverlap(boolean ignoreFileOverlap) {
    this.ignoreFileOverlap = ignoreFileOverlap;
  }
}
