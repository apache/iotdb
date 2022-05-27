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

package org.apache.iotdb;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RewriteBadFileTool {
  // backup data dir path
  private static String backUpDirPath = "backup";
  // validation file path
  private static String validationFilePath = "TsFile_validation_view.txt";
  // output file path
  private static String outputLogFilePath = "TsFile_rewrite_view.txt";

  private static final String HostIP = "localhost";
  private static final String rpcPort = "6667";
  private static final String user = "root";
  private static final String password = "root";

  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private static PrintWriter pw;

  /**
   * -b=[path of backUp directory] -v=[path of validation file] -o=[path of output log] -m=[whether
   * move the file]
   */
  public static void main(String[] args) throws IOException {
    if (!checkArgs(args)) {
      System.exit(1);
    }
    pw = new PrintWriter(new FileWriter(outputLogFilePath));
    try {
      moveAndRewriteBadFile();
    } catch (IoTDBConnectionException
        | IOException
        | StatementExecutionException
        | InterruptedException e) {
      e.printStackTrace();
    } finally {
      pw.close();
    }
  }

  public static void moveAndRewriteBadFile()
      throws IOException, IoTDBConnectionException, StatementExecutionException,
          InterruptedException {
    Session session = new Session(HostIP, rpcPort, user, password);
    session.open(false);

    BufferedReader bufferedReader = new BufferedReader(new FileReader(validationFilePath));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      if (!line.startsWith("-- Find the bad file ")) {
        continue;
      }
      String badFilePath = line.replace("-- Find the bad file ", "");

      printBoth(String.format("Start moving %s to backup dir.", badFilePath));
      session.executeNonQueryStatement(String.format("move '%s' '%s'", badFilePath, backUpDirPath));
      String[] dirs = badFilePath.split("/");
      String targetFilePath = backUpDirPath + File.separator + dirs[dirs.length - 1];
      File targetFile = new File(targetFilePath);
      // move mods file
      File modsFile = new File(badFilePath + ModificationFile.FILE_SUFFIX);
      if (modsFile.exists()) {
        fsFactory.moveFile(modsFile, new File(targetFilePath + ModificationFile.FILE_SUFFIX));
      }
      printBoth(String.format("Finish unloading %s.", badFilePath));
      // rewriteFile
      try {
        printBoth(String.format("Start rewriting %s to iotdb.", badFilePath));
        if (targetFile.exists()) {
          rewriteWrongTsFile(targetFilePath, session);
          targetFile.renameTo(new File(targetFilePath + "." + "finish"));
        } else {
          printBoth("---- Meet error in rewriting, " + targetFilePath + " does not exist.");
        }
      } catch (Throwable e) {
        e.printStackTrace();
        printBoth("---- Meet error in rewriting " + targetFilePath + ", " + e.getMessage());
      }
    }
    bufferedReader.close();
    session.close();
    printBoth("Finish rewriting all bad files.");
  }

  public static void rewriteWrongTsFile(String filename, Session session)
      throws IoTDBConnectionException, StatementExecutionException, IOException,
          IllegalPathException {
    // read mods file
    List<Modification> modifications = null;
    if (FSFactoryProducer.getFSFactory()
        .getFile(filename + ModificationFile.FILE_SUFFIX)
        .exists()) {
      modifications =
          (List<Modification>)
              new ModificationFile(filename + ModificationFile.FILE_SUFFIX).getModifications();
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      byte marker;
      String curDevice = null;
      long chunkHeaderOffset = -1;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            chunkHeaderOffset = reader.position() - 1;
            ChunkHeader header = reader.readChunkHeader(marker);
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            // 1. construct MeasurementSchema from chunkHeader
            String measurement = header.getMeasurementID();
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    measurement,
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());
            // 2. record data point of each measurement
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 =
                  new PageReader(
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              // read delete time range from old modification file
              List<TimeRange> deleteIntervalList =
                  getOldSortedDeleteIntervals(
                      curDevice, measurementSchema, chunkHeaderOffset, modifications);
              reader1.setDeleteIntervalList(deleteIntervalList);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              int maxRow;
              if (header.getChunkType() == MetaMarker.CHUNK_HEADER) {
                maxRow = (int) pageHeader.getNumOfValues();
              } else {
                maxRow = batchData.length();
              }
              Tablet tablet =
                  new Tablet(curDevice, Collections.singletonList(measurementSchema), maxRow);
              int rowIndex = 0;
              while (batchData.hasCurrent()) {
                tablet.addTimestamp(rowIndex, batchData.currentTime());
                tablet.addValue(measurement, rowIndex, batchData.currentValue());
                batchData.next();
                rowIndex++;
              }
              tablet.rowSize = rowIndex;
              session.insertTablet(tablet);
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            curDevice = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            reader.getMinPlanIndex();
            reader.getMaxPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
    }
  }

  private static boolean checkArgs(String[] args) {
    if (args.length != 3) {
      System.out.println(
          "Param incorrect, -b=[path of backUp directory] -v=[path of validation file] -o=[path of output file].");
      return false;
    }
    for (String arg : args) {
      if (arg.startsWith("-b")) {
        backUpDirPath = arg.split("=")[1];
      } else if (arg.startsWith("-v")) {
        validationFilePath = arg.split("=")[1];
      } else if (arg.startsWith("-o")) {
        outputLogFilePath = arg.split("=")[1];
      } else {
        System.out.println(
            "Param incorrect, -b=[path of backUp directory] -v=[path of validation file] -o=[path of output file].");
        return false;
      }
    }
    return true;
  }

  private static void printBoth(String msg) {
    System.out.println(msg);
    pw.println(msg);
  }

  private static List<TimeRange> getOldSortedDeleteIntervals(
      String deviceId,
      MeasurementSchema schema,
      long chunkHeaderOffset,
      List<Modification> modifications)
      throws IllegalPathException {
    if (modifications != null && modifications.size() != 0) {
      Iterator<Modification> modsIterator = modifications.listIterator();
      ChunkMetadata chunkMetadata = new ChunkMetadata();
      Deletion currentDeletion = null;
      while (modsIterator.hasNext()) {
        currentDeletion = (Deletion) modsIterator.next();
        // if deletion path match the chunkPath, then add the deletion to the list
        if (currentDeletion
                .getPath()
                .matchFullPath(new PartialPath(deviceId + "." + schema.getMeasurementId()))
            && currentDeletion.getFileOffset() > chunkHeaderOffset) {
          chunkMetadata.insertIntoSortedDeletions(
              currentDeletion.getStartTime(), currentDeletion.getEndTime());
        }
      }
      return chunkMetadata.getDeleteIntervalList();
    }
    return null;
  }
}
