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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
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
import org.apache.iotdb.tsfile.utils.Binary;
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

/**
 * This tool reads tsFiles and rewrites it chunk by chunk. It constructs tablet and invokes
 * insertTablet() for every page in chunk, in case chunk is too large. `Move` command is used to
 * unload files in iotdb, and mods files are moved manually.
 */
public class RewriteFileTool {
  // backup data dir path
  private static String backUpDirPath;
  // validation file path
  private static String validationFilePath;
  // tsfile list path
  private static String tsfileListPath;
  // output file path
  private static String outputLogFilePath;

  private static final String HostIP = "localhost";
  private static final String rpcPort = "6667";
  private static String user = "root";
  private static String password = "root";

  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private static final long MAX_TABLET_SIZE = 1024 * 1024;

  private static PrintWriter writer;

  /**
   * -b=[path of backUp directory] -vf=[path of validation file]/-f=[path of tsfile list] -o=[path
   * of output log] -u=[username, default="root"] -pw=[password, default="root"]
   */
  public static void main(String[] args) throws IOException {
    if (!checkArgs(args)) {
      System.exit(1);
    }
    writer = new PrintWriter(new FileWriter(outputLogFilePath));
    try {
      if (validationFilePath != null) {
        readValidationFile(validationFilePath);
      }
      if (tsfileListPath != null) {
        readTsFileList(tsfileListPath);
      }
    } catch (Exception e) {
      printBoth(e.getMessage());
      e.printStackTrace();
    } finally {
      writer.close();
    }
  }

  public static void readValidationFile(String validationFilePath)
      throws IOException, IoTDBConnectionException {
    Session session = new Session(HostIP, rpcPort, user, password);
    session.open(false);

    BufferedReader bufferedReader = new BufferedReader(new FileReader(validationFilePath));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      if (!line.startsWith("-- Find the bad file ")) {
        continue;
      }
      String badFilePath = line.replace("-- Find the bad file ", "");
      unloadAndReWriteWrongTsFile(badFilePath, session);
    }
    bufferedReader.close();
    session.close();
    printBoth("Finish rewriting all bad files.");
  }

  public static void readTsFileList(String tsfileListPath)
      throws IoTDBConnectionException, IOException {
    Session session = new Session(HostIP, rpcPort, user, password);
    session.open(false);

    BufferedReader bufferedReader = new BufferedReader(new FileReader(tsfileListPath));
    String badFilePath;
    while ((badFilePath = bufferedReader.readLine()) != null) {
      unloadAndReWriteWrongTsFile(badFilePath, session);
    }
    bufferedReader.close();
    session.close();
    printBoth("Finish rewriting all bad files.");
  }

  public static void unloadAndReWriteWrongTsFile(String filename, Session session) {
    try {
      String[] dirs = filename.split("/");
      String targetFilePath = backUpDirPath + File.separator + dirs[dirs.length - 1];
      File targetFile = new File(targetFilePath);
      // move mods file
      File modsFile = new File(filename + ModificationFile.FILE_SUFFIX);
      if (modsFile.exists()) {
        fsFactory.moveFile(modsFile, new File(targetFilePath + ModificationFile.FILE_SUFFIX));
      }
      if (targetFile.exists()) {
        printBoth(String.format("%s is already in the backup dir. Don't need to move.", filename));
      } else {
        printBoth(String.format("Start moving %s to backup dir.", filename));
        session.executeNonQueryStatement(String.format("move '%s' '%s'", filename, backUpDirPath));
      }
      printBoth(String.format("Finish unloading %s.", filename));

      // try to rewriteFile
      printBoth(String.format("Start rewriting %s to iotdb.", filename));
      if (targetFile.exists()) {
        rewriteWrongTsFile(targetFilePath, session);
        targetFile.renameTo(new File(targetFilePath + "." + "finish"));
      } else {
        printBoth("---- Meet error in rewriting, " + targetFilePath + " does not exist.");
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      printBoth("---- Meet error in unloading " + filename + ", " + e.getMessage());
    }
  }

  public static void rewriteWrongTsFile(String filename, Session session) {
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
              long curTabletSize = 0;
              while (batchData.hasCurrent()) {
                tablet.addTimestamp(tablet.rowSize, batchData.currentTime());
                tablet.addValue(measurement, tablet.rowSize, batchData.currentValue());
                tablet.rowSize++;
                // calculate curTabletSize based on timestamp and value
                curTabletSize += 8;
                switch (header.getDataType()) {
                  case BOOLEAN:
                    curTabletSize += 1;
                    break;
                  case INT32:
                  case FLOAT:
                    curTabletSize += 4;
                    break;
                  case INT64:
                  case DOUBLE:
                    curTabletSize += 8;
                    break;
                  case TEXT:
                    curTabletSize += 4 + ((Binary) batchData.currentValue()).getLength();
                    break;
                  default:
                    throw new UnSupportedDataTypeException(
                        String.format("Data type %s is not supported.", header.getDataType()));
                }
                // if curTabletSize is over the threshold
                if (curTabletSize >= MAX_TABLET_SIZE) {
                  session.insertTablet(tablet);
                  curTabletSize = 0;
                  tablet.reset();
                }
                batchData.next();
              }
              if (tablet.rowSize > 0) {
                session.insertTablet(tablet);
              }
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
    } catch (IllegalPathException
        | IOException
        | IoTDBConnectionException
        | StatementExecutionException e) {
      printBoth("---- Meet error in rewriting " + filename + ", " + e.getMessage());
      printBoth(e.getMessage());
      e.printStackTrace();
    }
  }

  private static boolean checkArgs(String[] args) {
    String paramConfig =
        "-b=[path of backUp directory] -vf=[path of validation file]/-f=[path of tsfile list] -o=[path of output log] -u=[username, default=\"root\"] -pw=[password, default=\"root\"]";
    for (String arg : args) {
      if (arg.startsWith("-b")) {
        backUpDirPath = arg.substring(arg.indexOf('=') + 1);
      } else if (arg.startsWith("-vf")) {
        validationFilePath = arg.substring(arg.indexOf('=') + 1);
      } else if (arg.startsWith("-f")) {
        tsfileListPath = arg.substring(arg.indexOf('=') + 1);
      } else if (arg.startsWith("-o")) {
        outputLogFilePath = arg.substring(arg.indexOf('=') + 1);
      } else if (arg.startsWith("-u")) {
        user = arg.substring(arg.indexOf('=') + 1);
      } else if (arg.startsWith("-pw")) {
        password = arg.substring(arg.indexOf('=') + 1);
      } else {
        System.out.println("Param incorrect!" + paramConfig);
        return false;
      }
    }
    if (backUpDirPath == null
        || (validationFilePath == null && tsfileListPath == null)
        || outputLogFilePath == null) {
      System.out.println("Param incorrect!" + paramConfig);
      return false;
    }
    return true;
  }

  private static void printBoth(String msg) {
    System.out.println(msg);
    writer.println(msg);
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
