package org.apache.iotdb.db.tools.validate;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileValidationTool {
  private static final boolean printDetails=true;

  private static final Logger logger = LoggerFactory.getLogger(TsFileValidationTool.class);
  private static String baseDataDirPath;
  private static int badFileNum=0;

  /**
   * Num of param should be three, which is [path of base data dir] [move file or not] [print device
   * or not]. Eg: xxx/iotdb/data false true
   */
  public static void main(String[] args) throws WriteProcessException, IOException {
    if (!checkArgs(args)) {
      System.exit(1);
    }
    System.out.println("Start checking seq files ...");
    // get seq data dirs
    List<String> seqDataDirs =
        new ArrayList<>(
            Arrays.asList(
                Objects.requireNonNull(
                    new File(baseDataDirPath)
                        .list((dir, name) -> (!name.equals("system") && !name.equals("wal"))))));
    for (int i = 0; i < Objects.requireNonNull(seqDataDirs).size(); i++) {
      seqDataDirs.set(
          i,
          baseDataDirPath
              + File.separator
              + seqDataDirs.get(i)
              + File.separator
              + IoTDBConstant.SEQUENCE_FLODER_NAME);
    }

    for (String seqDataPath : seqDataDirs) {
      // get sg data dirs
      File seqDataDir = new File(seqDataPath);
      if (!checkIsDirectory(seqDataDir)) {
        continue;
      }
      File[] sgDirs = seqDataDir.listFiles();
      for (File sgDir : Objects.requireNonNull(sgDirs)) {
        if (!checkIsDirectory(sgDir)) {
          continue;
        }
        System.out.println("- Check files in storage group: " + sgDir.getAbsolutePath());
        // get vsg data dirs
        File[] vsgDirs = sgDir.listFiles();
        for (File vsgDir : Objects.requireNonNull(vsgDirs)) {
          if (!checkIsDirectory(vsgDir)) {
            continue;
          }
          // get time partition dir
          File[] timePartitionDirs = vsgDir.listFiles();
          for (File timePartitionDir : Objects.requireNonNull(timePartitionDirs)) {
            if (!checkIsDirectory(timePartitionDir)) {
              continue;
            }
            // get all seq files under the time partition dir
            List<File> tsFiles =
                Arrays.asList(
                    Objects.requireNonNull(
                        timePartitionDir.listFiles(
                            file -> file.getName().endsWith(TSFILE_SUFFIX))));
            tsFiles.sort(
                (f1, f2) ->
                    Long.compareUnsigned(
                        Long.parseLong(f1.getName().split("-")[1]),
                        Long.parseLong(f2.getName().split("-")[1])));
            findUncorrectFiles(tsFiles);
          }
        }
      }
    }
    System.out.println("Finish checking successfully, totally find "+badFileNum+" bad files.");
  }

  private static void findUncorrectFiles(List<File> tsFiles) throws IOException {
    // measurementID -> lastTime
    Map<String, Long> measurementLastTime = new HashMap<>();
    // deviceID -> endTime, the endTime of device in the last seq file
    Map<String,Long> deviceEndTime=new HashMap<>();

    for (File tsFile : tsFiles) {
      TsFileResource resource=new TsFileResource(tsFile);
      if (!new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()) {
        // resource file does not exist, tsfile may not be flushed yet
        logger.warn(
            "{} does not exist ,skip it.",
            tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
        continue;
      }else{
        resource.deserialize();
      }
      boolean isBadFile = false;
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
        reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
        // timestamps of each page in one time chunk, which is used to get corresponding value in
        // value chunk.
        List<long[]> timeBatch = new ArrayList<>();
        int pageIndex = 0;
        byte marker;
        String deviceID = "";
        long lastChunkEndTime=Long.MIN_VALUE;
        Map<String,boolean[]> hasMeasurementPrintedDetails=new HashMap<>();
        // start reading data points in sequence
        while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
          switch (marker) {
            case MetaMarker.CHUNK_HEADER:
            case MetaMarker.TIME_CHUNK_HEADER:
            case MetaMarker.VALUE_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
              ChunkHeader header = reader.readChunkHeader(marker);
              if (header.getDataSize() == 0) {
                // empty value chunk
                break;
              }
              long currentChunkEndTime=Long.MIN_VALUE;
              String measurementID = deviceID + PATH_SEPARATOR + header.getMeasurementID();
              hasMeasurementPrintedDetails.computeIfAbsent(measurementID, k -> new boolean[4]);
              Decoder defaultTimeDecoder =
                  Decoder.getDecoderByType(
                      TSEncoding.valueOf(
                          TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                      TSDataType.INT64);
              Decoder valueDecoder =
                  Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
              int dataSize = header.getDataSize();
              pageIndex = 0;
              if (header.getDataType() == TSDataType.VECTOR) {
                timeBatch.clear();
              }
              long lastPageEndTime=Long.MIN_VALUE;
              while (dataSize > 0) {
                valueDecoder.reset();
                PageHeader pageHeader =
                    reader.readPageHeader(
                        header.getDataType(),
                        (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                long currentPageEndTime=Long.MIN_VALUE;
                if ((header.getChunkType() & (byte) TsFileConstant.TIME_COLUMN_MASK)
                    == (byte) TsFileConstant.TIME_COLUMN_MASK) {
                  // Time Chunk
                  TimePageReader timePageReader =
                      new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                  timeBatch.add(timePageReader.getNextTimeBatch());
                  for (int i = 0; i < timeBatch.get(pageIndex).length; i++) {
                    long timestamp=timeBatch.get(pageIndex)[i];
                    if (timestamp
                        <= measurementLastTime.getOrDefault(measurementID, Long.MIN_VALUE)) {
                      // find bad file
                      if (!isBadFile) {
                        System.out.println("-- Find the bad file " + tsFile.getAbsolutePath());
                        isBadFile = true;
                        badFileNum++;
                      }
                      if(printDetails){
                        if(!hasMeasurementPrintedDetails.get(measurementID)[0]&&timestamp<=deviceEndTime.getOrDefault(deviceID,Long.MIN_VALUE)){
                          System.out.println("---- "+measurementID+" overlap between files");
                          hasMeasurementPrintedDetails.get(measurementID)[0]=true;
                        }else if(!hasMeasurementPrintedDetails.get(measurementID)[1]&&timestamp<=lastChunkEndTime){
                          System.out.println("---- "+measurementID+" overlap between chunks");
                          hasMeasurementPrintedDetails.get(measurementID)[1]=true;
                        }else if(!hasMeasurementPrintedDetails.get(measurementID)[2]&&timestamp<=lastPageEndTime){
                          System.out.println("---- "+measurementID+" overlap between pages");
                          hasMeasurementPrintedDetails.get(measurementID)[2]=true;
                        }else if(!hasMeasurementPrintedDetails.get(measurementID)[3]){
                          System.out.println("---- "+measurementID+" overlap within one page");
                          hasMeasurementPrintedDetails.get(measurementID)[3]=true;
                        }
                      }
                    } else {
                      measurementLastTime.put(measurementID, timestamp);
                      currentChunkEndTime = timestamp;
                      currentPageEndTime=timestamp;
                    }
                  }
                } else if ((header.getChunkType() & (byte) TsFileConstant.VALUE_COLUMN_MASK)
                    == (byte) TsFileConstant.VALUE_COLUMN_MASK) {
                  // Value Chunk, skip it
                } else {
                  // NonAligned Chunk
                  PageReader pageReader =
                      new PageReader(
                          pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                  BatchData batchData = pageReader.getAllSatisfiedPageData();
                  while (batchData.hasCurrent()) {
                    long timestamp=batchData.currentTime();
                    if (timestamp
                        <= measurementLastTime.getOrDefault(measurementID, Long.MIN_VALUE)) {
                      // find bad file
                      if (!isBadFile) {
                        System.out.println("-- Find the bad file " + tsFile.getAbsolutePath());
                        isBadFile = true;
                        badFileNum++;
                      }
                      if(printDetails){
                        if(timestamp<=deviceEndTime.getOrDefault(deviceID,Long.MIN_VALUE)){
                          if (!hasMeasurementPrintedDetails.get(measurementID)[0]) {
                            System.out.println("---- " + measurementID + " overlap between files");
                            hasMeasurementPrintedDetails.get(measurementID)[0] = true;
                          }
                        }else if(timestamp<=lastChunkEndTime){
                          if (!hasMeasurementPrintedDetails.get(measurementID)[1]) {
                            System.out.println("---- " + measurementID + " overlap between chunks");
                            hasMeasurementPrintedDetails.get(measurementID)[1] = true;
                          }
                        }else if(timestamp<=lastPageEndTime){
                          if (!hasMeasurementPrintedDetails.get(measurementID)[2]) {
                            System.out.println("---- " + measurementID + " overlap between pages");
                            hasMeasurementPrintedDetails.get(measurementID)[2] = true;
                          }
                        }else {
                          if(!hasMeasurementPrintedDetails.get(measurementID)[3]){
                            System.out.println("---- "+measurementID+" overlap within one page");
                            hasMeasurementPrintedDetails.get(measurementID)[3]=true;
                          }
                        }
                      }
                    } else {
                      measurementLastTime.put(measurementID, timestamp);
                      currentChunkEndTime=timestamp;
                    }
                    batchData.next();
                  }
                }
                pageIndex++;
                dataSize -= pageHeader.getSerializedPageSize();
                lastPageEndTime=currentPageEndTime;
              }
              lastChunkEndTime=currentChunkEndTime;
              break;
            case MetaMarker.CHUNK_GROUP_HEADER:
              if(!deviceID.equals("")){
                // record the end time of last device in current file
                if(resource.getStartTime(deviceID)>deviceEndTime.getOrDefault(deviceID,Long.MIN_VALUE)){
                  deviceEndTime.put(deviceID,resource.getEndTime(deviceID));
                }
              }
              ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
              deviceID = chunkGroupHeader.getDeviceID();
              break;
            case MetaMarker.OPERATION_INDEX_RANGE:
              reader.readPlanIndex();
              break;
            default:
              MetaMarker.handleUnexpectedMarker(marker);
          }
        }
      }
    }
  }

  public static boolean checkArgs(String[] args) {
    if (args.length != 1) {
      System.out.println(
          "Num of param should be three, which is [path of base data dir] [move file or not] [print device or not]. Eg: xxx/iotdb/data false true");
      return false;
    } else {
      baseDataDirPath = args[0];
      if ((baseDataDirPath.endsWith("data") || baseDataDirPath.endsWith("data" + File.separator))
          && !baseDataDirPath.endsWith("data" + File.separator + "data")) {
        return true;
      }
      System.out.println("Please input correct base data dir. Eg: xxx/iotdb/data");
      return false;
    }
  }

  private static boolean checkIsDirectory(File dir) {
    boolean res = true;
    if (!dir.isDirectory()) {
      logger.error("{} is not a directory or does not exist, skip it.", dir.getAbsolutePath());
      res = false;
    }
    return res;
  }
}
