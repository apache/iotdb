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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
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
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileLoaderWithFileSeriesReader {
  private static final int MAX_TABLET_LENGTH = 1024 * 64;
  private static final long MAX_TABLET_SIZE = 1024 * 1024;

  private static String host = "localhost";
  private static String port = "6667";
  private static String user = "root";
  private static String password = "root";
  private static String filePath = "";

  private static Map<String, LinkedList<Tablet>> device2Tablets;
  private static Map<String, Set<MeasurementSchema>> device2Measurements;

  public static void main(String[] args) {
    Session session = null;
    try {
      parseArgs(args);
      session = new Session(host, port, user, password);
      session.open();
      System.out.printf("Connect to IoTDB %s:%s successfully.%n", host, port);
      writeToIoTDB(collectTsFiles(new File(filePath)), session);
    } catch (IoTDBConnectionException e) {
      System.out.printf("Can not connect to IoTDB. %s%n", e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      System.out.printf("Load Error. %s%n", e.getMessage());
      e.printStackTrace();
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          System.out.printf("Can not connect to IoTDB. %s%n", e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  public static void parseArgs(String[] args) {
    Options options = createOptions();
    try {
      CommandLine commandLine = new DefaultParser().parse(options, args);
      host = getArgOrDefault(commandLine, "h", host);
      port = getArgOrDefault(commandLine, "p", port);
      user = getArgOrDefault(commandLine, "u", user);
      password = getArgOrDefault(commandLine, "pw", password);
      filePath = getArgOrDefault(commandLine, "f", filePath);
    } catch (ParseException e) {
      System.out.printf("Parse Args Error. %s%n", e.getMessage());
      priHelp(options);
    }
  }

  private static void priHelp(Options options) {
    new HelpFormatter().printHelp("./load-tsfile.sh(load-tsfile.bat if Windows)", options, true);
  }

  private static String getArgOrDefault(
      CommandLine commandLine, String argName, String defaultValue) {
    String value = commandLine.getOptionValue(argName);
    return value == null ? defaultValue : value;
  }

  public static Options createOptions() {
    Options options = new Options();
    Option help = new Option("help", false, "Display help information(optional)");
    help.setRequired(false);
    options.addOption(help);

    Option host =
        Option.builder("h")
            .argName("host")
            .hasArg()
            .desc("Host Name (optional, default 127.0.0.1)")
            .build();
    options.addOption(host);

    Option port =
        Option.builder("p").argName("port").hasArg().desc("Port (optional, default 6667)").build();
    options.addOption(port);

    Option username =
        Option.builder("u")
            .argName("username")
            .hasArg()
            .desc("User name (required)")
            .required()
            .build();
    options.addOption(username);

    Option password =
        Option.builder("pw").argName("password").hasArg().desc("password (optional)").build();
    options.addOption(password);

    Option filePathOpt =
        Option.builder("f")
            .argName("file")
            .hasArg()
            .desc("File or Dictionary to be loaded.")
            .required()
            .build();
    options.addOption(filePathOpt);
    return options;
  }

  public static List<File> collectTsFiles(File file) {
    if (file.isFile()) {
      return file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)
          ? Collections.singletonList(file)
          : Collections.emptyList();
    }
    List<File> list = new ArrayList<>();
    for (File listFile : file.listFiles()) {
      if (listFile.isDirectory()) {
        list.addAll(collectTsFiles(listFile));
      } else if (listFile.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
        list.add(listFile);
      }
    }
    return list;
  }

  public static void writeToIoTDB(List<File> files, Session session) {
    int size = files.size();
    List<File> unloadTsFiles = new ArrayList<>();
    System.out.printf("Collect TsFiles successfully, %d files to be loaded.%n", size);
    System.out.println("Start Loading TsFiles...");
    for (int i = 0; i < size; i++) {
      File file = files.get(i);
      System.out.printf("Loading %s(%d/%d)...", file.getPath(), i + 1, size);
      try {
        writeTsFile(file.getPath(), session);
      } catch (Exception e) {
        System.out.println("Error");
        System.out.println(
            "------------------------------Error Message------------------------------");
        e.printStackTrace();
        System.out.println(
            "------------------------------End Message------------------------------");
        unloadTsFiles.add(file);
        continue;
      }
      System.out.println("Done");
    }
    System.out.println("Finish Loading TsFiles");
    System.out.printf(
        "Load %d TsFiles successfully, %d TsFiles not loaded.%n",
        size - unloadTsFiles.size(), unloadTsFiles.size());
    if (!unloadTsFiles.isEmpty()) {
      System.out.println("Load Error TsFiles list");
      for (File file : unloadTsFiles) {
        System.out.println(file.getPath());
      }
    }
  }

  /**
   * Read a TsFile and write into IoTDB session. This method can load TsFile with IoTDB version
   * 0.12-0.14
   *
   * @param filename the file path to be loaded
   * @param session IoTDB session
   */
  public static void writeTsFile(String filename, Session session)
      throws IOException, IllegalPathException, IoTDBConnectionException,
          StatementExecutionException {
    // read mods file
    List<Modification> modifications = null;
    if (FSFactoryProducer.getFSFactory()
            .getFile(filename + ModificationFile.FILE_SUFFIX)
            .exists()) {
      modifications =
              (List<Modification>)
                      new ModificationFile(filename + ModificationFile.FILE_SUFFIX).getModifications();
    }

    parseDeviceFromTsFile(filename);

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      byte marker;
      List<long[]> timeBatch = new ArrayList<>();
      String curDevice = null;
      long curMemorySize = 0;
      long chunkHeaderOffset = -1;
      boolean isAligned = false;
      Tablet curTablet = null;
      Map<Long, Integer> timestamp2Row = null;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
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
            int measurementIndex = curTablet.getSchemas().indexOf(measurementSchema);
            // 2. record data point of each measurement
            int dataSize = header.getDataSize();
            int pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              isAligned = true;
              timeBatch.clear();
            }
            List<TimeRange> deleteIntervalList =
                    getSortedDeleteIntervals(
                            curDevice, measurementSchema, chunkHeaderOffset, modifications);
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                      reader.readPageHeader(
                              header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                      == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                TimePageReader timePageReader =
                        new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timePageReader.setDeleteIntervalList(deleteIntervalList);
                timeBatch.add(timePageReader.getNextTimeBatch());
                for (int i = 0; i < timeBatch.get(pageIndex).length; i++) {
                  int rowIndex = curTablet.rowSize++;
                  long timestamp = timeBatch.get(pageIndex)[i];
                  curTablet.addTimestamp(rowIndex, timestamp);
                  timestamp2Row.put(timestamp, rowIndex);
                }
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                      == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                ValuePageReader valuePageReader =
                        new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                valuePageReader.setDeleteIntervalList(deleteIntervalList);
                TsPrimitiveType[] valueBatch =
                        valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
                for (int i = 0; i < timeBatch.get(pageIndex).length; i++) {
                  int rowIndex = timestamp2Row.get(timeBatch.get(pageIndex)[i]);
                  curTablet.addValue(measurement, rowIndex, valueBatch[i].getValue());
                  curTablet.bitMaps[measurementIndex].unmark(rowIndex);
                }
              } else { // NonAligned Chunk
                PageReader reader1 =
                        new PageReader(
                                pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                // read delete time range from old modification file
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
              }
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            if (curTablet != null) {
              if (isAligned) {
                session.insertAlignedTablet(curTablet);
              } else {
                session.insertTablet(curTablet);
              }
            }
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            curDevice = chunkGroupHeader.getDeviceID();
            curMemorySize = 0;
            isAligned = false;
            timestamp2Row = new HashMap<>();
            curTablet = device2Tablets.get(curDevice).pollFirst();
            curTablet.initBitMaps();
            for (BitMap bitMap : curTablet.bitMaps) {
              bitMap.markAll();
            }
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

  private static void parseDeviceFromTsFile(String filename) throws IOException {
    device2Tablets = new HashMap<>();
    device2Measurements = new HashMap<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      String curDevice = null;
      byte marker;
      List<MeasurementSchema> measurementSchemas = null;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    header.getMeasurementID(),
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());
            device2Measurements
                .computeIfAbsent(curDevice, o -> new HashSet<>())
                .add(measurementSchema);
            if (header.getDataType() != TSDataType.VECTOR) {
              measurementSchemas.add(measurementSchema);
            }
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              reader.readPage(pageHeader, header.getCompressionType());
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            if (measurementSchemas != null) {
              device2Tablets
                  .computeIfAbsent(curDevice, o -> new LinkedList<>())
                  .add(new Tablet(curDevice, measurementSchemas, MAX_TABLET_LENGTH));
            }
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            curDevice = chunkGroupHeader.getDeviceID();
            measurementSchemas = new ArrayList<>();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      if (measurementSchemas != null) {
        device2Tablets
            .computeIfAbsent(curDevice, o -> new LinkedList<>())
            .add(new Tablet(curDevice, measurementSchemas, MAX_TABLET_LENGTH));
      }
    }
  }

  private static List<TimeRange> getSortedDeleteIntervals(
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
