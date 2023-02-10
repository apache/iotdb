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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileAlignedSeriesReaderIterator;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RewriteTsFileTool {
  private static final int MAX_TABLET_LENGTH = 1024 * 64;

  private static String host = "localhost";
  private static String port = "6667";
  private static String user = "root";
  private static String password = "root";
  private static String filePath = "";
  private static String readMode = "s";
  private static boolean ignoreBrokenChunk = false;

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
      readMode = getArgOrDefault(commandLine, "rm", readMode);
      ignoreBrokenChunk = commandLine.hasOption("ig");
    } catch (ParseException e) {
      System.out.printf("Parse Args Error. %s%n", e.getMessage());
      priHelp(options);
    }
  }

  private static void priHelp(Options options) {
    new HelpFormatter()
        .printHelp("./rewrite-tsfile.sh(rewrite-tsfile.bat if Windows)", options, true);
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

    Option readModeOpt =
        Option.builder("rm")
            .argName("readMode")
            .hasArg()
            .desc("Read mode, s(equence) or r(everse)")
            .required()
            .build();
    options.addOption(readModeOpt);

    options.addOption("ig", "ignore-broken chunks");
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

  /**
   * write a list of file to IoTDB with session.
   *
   * @param files a list of file to write to IoTDB
   * @param session IoTDB session
   */
  public static void writeToIoTDB(List<File> files, Session session) {
    sortTsFiles(files);
    System.out.printf("Collect TsFiles successfully, %d files to be loaded.%n", files.size());
    System.out.println("Start Loading TsFiles...");
    if (readMode.equals("s")) {
      writeTsFileSequentially(files, session);
    } else {
      try {
        reverseWriteTsFile(files, session);
      } catch (IOException
          | IllegalPathException
          | IoTDBConnectionException
          | StatementExecutionException
          | NoMeasurementException e) {
        System.out.println(
            "------------------------------Error Message------------------------------");
        e.printStackTrace();
        System.out.println(
            "------------------------------End Message------------------------------");
      }
    }
    System.out.println("Finish Loading TsFiles");
  }

  private static void writeTsFileSequentially(List<File> files, Session session) {
    int size = files.size();
    List<File> unloadTsFiles = new ArrayList<>();
    for (int i = 0; i < files.size(); i++) {
      File file = files.get(i);
      System.out.printf("Loading %s(%d/%d)...", file.getPath(), i + 1, size);
      try {
        seqWriteSingleTsFile(file.getPath(), session);
        session.executeNonQueryStatement("FLUSH");
      } catch (Exception e) {
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

  private static void sortTsFiles(List<File> files) {
    Map<File, Long> file2Timestamp = new HashMap<>();
    Map<File, Long> file2Version = new HashMap<>();
    for (File file : files) {
      String[] splitStrings = file.getName().split(FilePathUtils.FILE_NAME_SEPARATOR);
      file2Timestamp.put(file, Long.parseLong(splitStrings[0]));
      file2Version.put(file, Long.parseLong(splitStrings[1]));
    }

    Collections.sort(
        files,
        (o1, o2) -> {
          long timestampDiff = file2Timestamp.get(o1) - file2Timestamp.get(o2);
          if (timestampDiff != 0) {
            return (int) (timestampDiff);
          }
          return (int) (file2Version.get(o1) - file2Version.get(o2));
        });
  }

  /**
   * Read a TsFile and write into IoTDB session. This method can load TsFile with IoTDB version.
   * Support TsFile generated from IoTDB version 0.12 - 0.14(including Aligned Timeseries).
   *
   * @param filename the file path to be loaded
   * @param session IoTDB session
   */
  public static void seqWriteSingleTsFile(String filename, Session session)
      throws IOException, IllegalPathException, IoTDBConnectionException,
          StatementExecutionException, NoMeasurementException {

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      if (!ignoreBrokenChunk) {
        long status = reader.selfCheck(new HashMap<>(), new ArrayList<>(), true);
        if (status == TsFileCheckStatus.INCOMPATIBLE_FILE
            || status == TsFileCheckStatus.FILE_EXISTS_MISTAKES) {
          throw new IOException(
              String.format(
                  "The file %s is incompatible, cannot rewrite it to IoTDB. If you want to rewrite "
                      + "all the good chunks in the file, retry with option -ignore-broken.",
                  filename));
        }
      }
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      String currentDevice = null;
      boolean isAlignedChunk = false;
      List<List<TsPrimitiveType>> valueForAlignedSeries = new ArrayList<>();
      List<Long> timeForAlignedSeries = new ArrayList<>();
      List<MeasurementSchema> schemaForAlignedSeries = new ArrayList<>();
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
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }
            boolean addSchema = false;
            List<TsPrimitiveType> valueList = new ArrayList<>();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              if ((header.getChunkType() & (byte) TsFileConstant.TIME_COLUMN_MASK)
                  == (byte) TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                readAndSendTimePage(
                    addSchema,
                    schemaForAlignedSeries,
                    header,
                    pageHeader,
                    pageData,
                    defaultTimeDecoder,
                    timeBatch,
                    pageIndex,
                    timeForAlignedSeries);
                addSchema = true;
                isAlignedChunk = true;
              } else if ((header.getChunkType() & (byte) TsFileConstant.VALUE_COLUMN_MASK)
                  == (byte) TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                readAndSendValuePage(
                    addSchema,
                    schemaForAlignedSeries,
                    header,
                    pageHeader,
                    pageData,
                    valueDecoder,
                    timeBatch,
                    pageIndex,
                    valueList);
                addSchema = true;
              } else { // NonAligned Chunk
                readAndSendSingleSeriesPage(
                    currentDevice, header, pageData, valueDecoder, defaultTimeDecoder, session);
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }
            if (isAlignedChunk && header.getDataType() != TSDataType.VECTOR) {
              valueForAlignedSeries.add(valueList);
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // get the next chunk group
            if (isAlignedChunk) {
              Tablet tablet = new Tablet(currentDevice, schemaForAlignedSeries, MAX_TABLET_LENGTH);
              for (int i = 0; i < timeForAlignedSeries.size(); ++i) {
                tablet.addTimestamp(tablet.rowSize, timeForAlignedSeries.get(i));
                for (int j = 0; j < valueForAlignedSeries.size(); ++j) {
                  if (valueForAlignedSeries.get(j).get(i) == null) {
                    continue;
                  }
                  switch (valueForAlignedSeries.get(j).get(i).getDataType()) {
                    case INT32:
                      tablet.addValue(
                          schemaForAlignedSeries.get(j).getMeasurementId(),
                          tablet.rowSize,
                          valueForAlignedSeries.get(j).get(i).getInt());
                      break;
                    case INT64:
                      tablet.addValue(
                          schemaForAlignedSeries.get(j).getMeasurementId(),
                          tablet.rowSize,
                          valueForAlignedSeries.get(j).get(i).getLong());
                      break;
                    case TEXT:
                      tablet.addValue(
                          schemaForAlignedSeries.get(j).getMeasurementId(),
                          tablet.rowSize,
                          valueForAlignedSeries.get(j).get(i).getStringValue());
                      break;
                    case BOOLEAN:
                      tablet.addValue(
                          schemaForAlignedSeries.get(j).getMeasurementId(),
                          tablet.rowSize,
                          valueForAlignedSeries.get(j).get(i).getBoolean());
                      break;
                    case FLOAT:
                      tablet.addValue(
                          schemaForAlignedSeries.get(j).getMeasurementId(),
                          tablet.rowSize,
                          valueForAlignedSeries.get(j).get(i).getFloat());
                      break;
                    case DOUBLE:
                      tablet.addValue(
                          schemaForAlignedSeries.get(j).getMeasurementId(),
                          tablet.rowSize,
                          valueForAlignedSeries.get(j).get(i).getDouble());
                      break;
                  }
                }
                tablet.rowSize++;
                if (tablet.rowSize >= MAX_TABLET_LENGTH) {
                  session.insertAlignedTablet(tablet);
                  tablet.reset();
                }
              }
              if (tablet.rowSize >= 0) {
                session.insertAlignedTablet(tablet);
                tablet.reset();
              }
              timeForAlignedSeries.clear();
              valueForAlignedSeries.clear();
              schemaForAlignedSeries.clear();
            }
            isAlignedChunk = false;
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            currentDevice = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            System.out.printf(
                "Cannot handle marker %d in position %d, stop reading %s%n",
                marker, reader.position(), filename);
        }
      }

      if (isAlignedChunk) {
        Tablet tablet = new Tablet(currentDevice, schemaForAlignedSeries, MAX_TABLET_LENGTH);
        for (int i = 0; i < timeForAlignedSeries.size(); ++i) {
          tablet.addTimestamp(tablet.rowSize, timeForAlignedSeries.get(i));
          for (int j = 0; j < valueForAlignedSeries.size(); ++j) {
            if (valueForAlignedSeries.get(j).get(i) == null) {
              continue;
            }
            switch (valueForAlignedSeries.get(j).get(i).getDataType()) {
              case INT32:
                tablet.addValue(
                    schemaForAlignedSeries.get(j).getMeasurementId(),
                    tablet.rowSize,
                    valueForAlignedSeries.get(j).get(i).getInt());
                break;
              case INT64:
                tablet.addValue(
                    schemaForAlignedSeries.get(j).getMeasurementId(),
                    tablet.rowSize,
                    valueForAlignedSeries.get(j).get(i).getLong());
                break;
              case TEXT:
                tablet.addValue(
                    schemaForAlignedSeries.get(j).getMeasurementId(),
                    tablet.rowSize,
                    valueForAlignedSeries.get(j).get(i).getStringValue());
                break;
              case BOOLEAN:
                tablet.addValue(
                    schemaForAlignedSeries.get(j).getMeasurementId(),
                    tablet.rowSize,
                    valueForAlignedSeries.get(j).get(i).getBoolean());
                break;
              case FLOAT:
                tablet.addValue(
                    schemaForAlignedSeries.get(j).getMeasurementId(),
                    tablet.rowSize,
                    valueForAlignedSeries.get(j).get(i).getFloat());
                break;
              case DOUBLE:
                tablet.addValue(
                    schemaForAlignedSeries.get(j).getMeasurementId(),
                    tablet.rowSize,
                    valueForAlignedSeries.get(j).get(i).getDouble());
                break;
            }
          }
          tablet.rowSize++;
          if (tablet.rowSize >= MAX_TABLET_LENGTH) {
            session.insertAlignedTablet(tablet);
            tablet.reset();
          }
        }
        if (tablet.rowSize >= 0) {
          session.insertAlignedTablet(tablet);
          tablet.reset();
        }
        timeForAlignedSeries.clear();
        valueForAlignedSeries.clear();
        schemaForAlignedSeries.clear();
      }
    }

    writeModification(filename, session);
  }

  private static void readAndSendValuePage(
      boolean addSchema,
      List<MeasurementSchema> schemaForAlignedSeries,
      ChunkHeader header,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Decoder valueDecoder,
      List<long[]> timeBatch,
      int pageIndex,
      List<TsPrimitiveType> valueList) {
    if (!addSchema && header.getDataType() != TSDataType.VECTOR) {
      schemaForAlignedSeries.add(
          new MeasurementSchema(
              header.getMeasurementID(),
              header.getDataType(),
              header.getEncodingType(),
              header.getCompressionType()));
    }
    ValuePageReader valuePageReader =
        new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
    TsPrimitiveType[] valueBatch = valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
    valueList.addAll(Arrays.asList(valueBatch));
  }

  private static void readAndSendTimePage(
      boolean addSchema,
      List<MeasurementSchema> schemaForAlignedSeries,
      ChunkHeader header,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Decoder defaultTimeDecoder,
      List<long[]> timeBatch,
      int pageIndex,
      List<Long> timeForAlignedSeries)
      throws IOException {
    if (!addSchema && header.getDataType() != TSDataType.VECTOR) {
      schemaForAlignedSeries.add(
          new MeasurementSchema(
              header.getMeasurementID(),
              header.getDataType(),
              header.getEncodingType(),
              header.getCompressionType()));
    }
    TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
    timeBatch.add(timePageReader.getNextTimeBatch());
    for (int i = 0; i < timeBatch.get(pageIndex).length; i++) {
      timeForAlignedSeries.add(timeBatch.get(pageIndex)[i]);
    }
  }

  private static void readAndSendSingleSeriesPage(
      String currentDevice,
      ChunkHeader header,
      ByteBuffer pageData,
      Decoder valueDecoder,
      Decoder defaultTimeDecoder,
      Session session)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    String measurementId = header.getMeasurementID();
    Tablet tablet =
        new Tablet(
            currentDevice,
            Collections.singletonList(
                new MeasurementSchema(
                    measurementId,
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType())),
            MAX_TABLET_LENGTH);
    PageReader pageReader =
        new PageReader(pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    while (batchData.hasCurrent()) {
      tablet.addTimestamp(tablet.rowSize, batchData.currentTime());
      tablet.addValue(measurementId, tablet.rowSize++, batchData.currentValue());
      if (tablet.rowSize >= MAX_TABLET_LENGTH) {
        session.insertTablet(tablet);
        tablet.reset();
      }
      batchData.next();
    }
    if (tablet.rowSize > 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void writeModification(String filename, Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    List<Modification> modifications = null;
    if (FSFactoryProducer.getFSFactory()
        .getFile(filename + ModificationFile.FILE_SUFFIX)
        .exists()) {
      modifications =
          (List<Modification>)
              new ModificationFile(filename + ModificationFile.FILE_SUFFIX).getModifications();
      for (Modification modification : modifications) {
        session.executeNonQueryStatement(
            String.format(
                "delete from %s.%s where time >= %d and time <= %d",
                modification.getDevice(),
                modification.getMeasurement(),
                ((Deletion) modification).getStartTime(),
                ((Deletion) modification).getEndTime()));
      }
    }
  }

  /**
   * Read the chunk metadata first, then read the chunk according to chunk metadata
   *
   * @param files
   * @param session
   * @throws IOException
   * @throws IllegalPathException
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   * @throws NoMeasurementException
   */
  public static void reverseWriteTsFile(List<File> files, Session session)
      throws IOException, IllegalPathException, IoTDBConnectionException,
          StatementExecutionException, NoMeasurementException {
    List<TsFileResource> resources = new ArrayList<>();
    files.forEach(x -> resources.add(new TsFileResource(x)));
    for (TsFileResource resource : resources) {
      try (MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(Collections.singletonList(resource))) {
        while (deviceIterator.hasNextDevice()) {
          Pair<String, Boolean> devicePair = deviceIterator.nextDevice();
          String device = devicePair.left;
          boolean isAligned = devicePair.right;
          if (isAligned) {
            try {
              writeAlignedSeries(device, deviceIterator, session);
            } catch (Throwable t) {
              // this is a broken aligned chunk, skip it
              System.out.println("Skip aligned chunk " + device);
            }
          } else {
            MultiTsFileDeviceIterator.MeasurementIterator seriesIterator =
                deviceIterator.iterateNotAlignedSeries(device, true);
            while (seriesIterator.hasNextSeries()) {
              writeSingleSeries(device, seriesIterator, session);
            }
          }
        }
      }
      writeModification(resource.getTsFile().getAbsolutePath(), session);
    }
  }

  /** Read data from tsfile and write it to IoTDB for a single not aligned series. */
  protected static void writeSingleSeries(
      String device, MultiTsFileDeviceIterator.MeasurementIterator seriesIterator, Session session)
      throws IllegalPathException {
    PartialPath p = new PartialPath(device, seriesIterator.nextSeries());
    LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList =
        seriesIterator.getMetadataListForCurrentSeries();
    while (!readerAndChunkMetadataList.isEmpty()) {
      Pair<TsFileSequenceReader, List<ChunkMetadata>> readerMetadataPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerMetadataPair.left;
      List<ChunkMetadata> chunkMetadataList = readerMetadataPair.right;
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        try {
          writeSingleChunk(device, p, chunkMetadata, reader, session);
        } catch (Throwable t) {
          // this is a broken chunk, skip it
          t.printStackTrace();
          System.out.printf("Skip broken chunk in device %s.%s%n", device, p.getMeasurement());
        }
      }
    }
  }

  /** Read and write a single chunk for not aligned series. */
  protected static void writeSingleChunk(
      String device,
      PartialPath p,
      ChunkMetadata chunkMetadata,
      TsFileSequenceReader reader,
      Session session)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    Chunk chunk = reader.readMemChunk(chunkMetadata);
    ChunkHeader chunkHeader = chunk.getHeader();
    MeasurementSchema schema =
        new MeasurementSchema(
            p.getMeasurement(),
            chunkHeader.getDataType(),
            chunkHeader.getEncodingType(),
            chunkHeader.getCompressionType());
    Tablet tablet = new Tablet(device, Collections.singletonList(schema), MAX_TABLET_LENGTH);
    IChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
      while (batchIterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
        tablet.addTimestamp(tablet.rowSize, timeValuePair.getTimestamp());
        switch (timeValuePair.getValue().getDataType()) {
          case TEXT:
            tablet.addValue(
                p.getMeasurement(), tablet.rowSize++, timeValuePair.getValue().getStringValue());
            break;
          case BOOLEAN:
            tablet.addValue(
                p.getMeasurement(), tablet.rowSize++, timeValuePair.getValue().getBoolean());
            break;
          case DOUBLE:
            tablet.addValue(
                p.getMeasurement(), tablet.rowSize++, timeValuePair.getValue().getDouble());
            break;
          case FLOAT:
            tablet.addValue(
                p.getMeasurement(), tablet.rowSize++, timeValuePair.getValue().getFloat());
            break;
          case INT64:
            tablet.addValue(
                p.getMeasurement(), tablet.rowSize++, timeValuePair.getValue().getLong());
            break;
          case INT32:
            tablet.addValue(
                p.getMeasurement(), tablet.rowSize++, timeValuePair.getValue().getInt());
            break;
        }
        if (tablet.rowSize >= MAX_TABLET_LENGTH) {
          session.insertTablet(tablet);
          tablet.reset();
        }
      }
    }
    if (tablet.rowSize > 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  /** Collect the schema list for an aligned device. */
  private static List<IMeasurementSchema> collectSchemaFromAlignedChunkMetadataList(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList)
      throws IOException {
    Set<MeasurementSchema> schemaSet = new HashSet<>();
    Set<String> measurementSet = new HashSet<>();
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair :
        readerAndChunkMetadataList) {
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        List<IChunkMetadata> valueChunkMetadataList =
            alignedChunkMetadata.getValueChunkMetadataList();
        for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
          if (chunkMetadata == null) {
            continue;
          }
          if (measurementSet.contains(chunkMetadata.getMeasurementUid())) {
            continue;
          }
          measurementSet.add(chunkMetadata.getMeasurementUid());
          Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetadata);
          ChunkHeader header = chunk.getHeader();
          schemaSet.add(
              new MeasurementSchema(
                  header.getMeasurementID(),
                  header.getDataType(),
                  header.getEncodingType(),
                  header.getCompressionType()));
        }
      }
    }
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaSet);
    schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return schemaList;
  }

  /** Read and write an aligned series. */
  protected static void writeAlignedSeries(
      String device, MultiTsFileDeviceIterator deviceIterator, Session session)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList =
        deviceIterator.getReaderAndChunkMetadataForCurrentAlignedSeries();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    List<IMeasurementSchema> iSchemaList =
        collectSchemaFromAlignedChunkMetadataList(readerAndChunkMetadataList);
    iSchemaList.forEach(x -> schemaList.add((MeasurementSchema) x));
    while (readerAndChunkMetadataList.size() > 0) {
      Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;
      TsFileAlignedSeriesReaderIterator readerIterator =
          new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadataList, iSchemaList);
      writeAlignedChunk(readerIterator, device, schemaList, session);
    }
  }

  private static void writeAlignedChunk(
      TsFileAlignedSeriesReaderIterator readerIterator,
      String device,
      List<MeasurementSchema> schemaList,
      Session session)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    while (readerIterator.hasNext()) {
      Tablet tablet = new Tablet(device, schemaList, MAX_TABLET_LENGTH);
      TsFileAlignedSeriesReaderIterator.NextAlignedChunkInfo readerInfo =
          readerIterator.nextReader();
      AlignedChunkReader alignedChunkReader = readerInfo.getReader();
      while (alignedChunkReader.hasNextSatisfiedPage()) {
        IBatchDataIterator batchDataIterator =
            alignedChunkReader.nextPageData().getBatchDataIterator();
        while (batchDataIterator.hasNext()) {
          tablet.addTimestamp(tablet.rowSize, batchDataIterator.currentTime());
          TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
          for (int i = 0; i < schemaList.size(); ++i) {
            if (pointsData[i] == null) {
              continue;
            }
            switch (pointsData[i].getDataType()) {
              case INT32:
                tablet.addValue(
                    schemaList.get(i).getMeasurementId(), tablet.rowSize, pointsData[i].getInt());
                break;
              case INT64:
                tablet.addValue(
                    schemaList.get(i).getMeasurementId(), tablet.rowSize, pointsData[i].getLong());
                break;
              case FLOAT:
                tablet.addValue(
                    schemaList.get(i).getMeasurementId(), tablet.rowSize, pointsData[i].getFloat());
                break;
              case DOUBLE:
                tablet.addValue(
                    schemaList.get(i).getMeasurementId(),
                    tablet.rowSize,
                    pointsData[i].getDouble());
                break;
              case BOOLEAN:
                tablet.addValue(
                    schemaList.get(i).getMeasurementId(),
                    tablet.rowSize,
                    pointsData[i].getBoolean());
                break;
              case TEXT:
                tablet.addValue(
                    schemaList.get(i).getMeasurementId(),
                    tablet.rowSize,
                    pointsData[i].getStringValue());
                break;
            }
          }
          tablet.rowSize++;
          batchDataIterator.next();
          if (tablet.rowSize >= MAX_TABLET_LENGTH) {
            session.insertAlignedTablet(tablet);
            tablet.reset();
          }
        }
      }
      if (tablet.rowSize > 0) {
        session.insertAlignedTablet(tablet);
        tablet.reset();
      }
    }
  }
}
