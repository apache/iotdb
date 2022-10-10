package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.FileElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.performer.impl.NewFastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.PointPriorityReader;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public class NewFastCompactionPerformerSubTask implements Callable<Void> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  @FunctionalInterface
  public interface RemovePage {
    void call(PageElement pageElement, List<PageElement> newOverlappedPages)
        throws WriteProcessException, IOException, IllegalPathException;
  }

  private final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  private final PriorityQueue<PageElement> pageQueue;

  private NewFastCrossCompactionWriter compactionWriter;

  private NewFastCompactionPerformer newFastCompactionPerformer;

  private int subTaskId;

  // the indexs of the timseries to be compacted to which the current sub thread is assigned
  private List<Integer> pathsIndex;

  // chunk metadata list of all timeseries of this device in this seq file
  private List<List<IChunkMetadata>> allSensorMetadatas;

  private List<AlignedChunkMetadata> alignedSensorMetadatas;

  private List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private Map<String, IMeasurementSchema> measurementSchemaMap = new LinkedHashMap<>();

  private List<String> allMeasurements = new ArrayList<>();

  // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  Map<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>>
      timeseriesSchemaAndMetadataOffsetMap;

  private final PointPriorityReader pointPriorityReader = new PointPriorityReader(this::removePage);

  // sorted source files by the start time of device
  private List<FileElement> fileList = new ArrayList<>();


  private boolean isAligned;

  private String deviceId;

  private int currentSensorIndex = 0;

  boolean hasStartMeasurement = false;

  private NewFastCompactionPerformerSubTask(
      List<IMeasurementSchema> measurementSchemas,
      boolean isAligned,
      NewFastCrossCompactionWriter compactionWriter,
      int subTaskId) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = subTaskId;
    this.measurementSchemas = measurementSchemas;
    this.isAligned = isAligned;
    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });

    pageQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });
  }

  /** Used for constructing nonAligned timeseries compaction. */
  public NewFastCompactionPerformerSubTask(
      NewFastCrossCompactionWriter compactionWriter,
      String deviceId,
      List<Integer> pathsIndex,
      List<String> allMeasurements,
      NewFastCompactionPerformer newFastCompactionPerformer,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      int subTaskId) {
    this.compactionWriter = compactionWriter;
    this.newFastCompactionPerformer = newFastCompactionPerformer;
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.pathsIndex = pathsIndex;
    this.deviceId = deviceId;
    this.subTaskId = subTaskId;
    this.allMeasurements = allMeasurements;
    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });

    pageQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });
  }

  /** Used for constructing aligned timeseries compaction. */
  public NewFastCompactionPerformerSubTask(
      NewFastCrossCompactionWriter compactionWriter,
      NewFastCompactionPerformer newFastCompactionPerformer,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      List<IMeasurementSchema> measurementSchemas,
      String deviceId,
      int subTaskId) {
    this(null, true, compactionWriter, subTaskId);
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.measurementSchemas = measurementSchemas;
    this.newFastCompactionPerformer = newFastCompactionPerformer;
    this.deviceId = deviceId;
  }

  @Override
  public Void call()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    if (isAligned) {
      int chunkMetadataPriority = 0;
      // put aligned chunk metadatas into queue
      // for (AlignedChunkMetadata alignedChunkMetadata : alignedSensorMetadatas) {
      //        chunkMetadataQueue.add(
      //            new ChunkMetadataElement(alignedChunkMetadata, chunkMetadataPriority++));
      // }
      newFastCompactionPerformer
          .getSortedSourceFilesAndFirstMeasurementNodeOfCurrentDevice()
          .forEach(x -> fileList.add(new FileElement(x.left, x.right)));

      compactionWriter.startMeasurement(measurementSchemas, subTaskId);
      compactFiles();
      compactionWriter.endMeasurement(subTaskId);

    } else {
      for (Integer index : pathsIndex) {
        newFastCompactionPerformer
            .getSortedSourceFiles()
            .forEach(x -> fileList.add(new FileElement(x)));

        currentSensorIndex = index;
        hasStartMeasurement = false;

        compactFiles();
        if (hasStartMeasurement) {
          compactionWriter.endMeasurement(subTaskId);
        }
      }
    }
    return null;

  }

  private void compactFiles()
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    while (!fileList.isEmpty()) {
      List<FileElement> overlappedFiles = findOverlapFiles(fileList.get(0));

      // read chunk metadatas from files and put them into chunk metadata queue
      deserializeFileIntoQueue(overlappedFiles);

      if (!isAligned) {
        startMeasurement();
      }

      compactChunks();
    }
  }

  private void startMeasurement() throws IOException {

      if (!hasStartMeasurement && !chunkMetadataQueue.isEmpty()) {
        ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
        MeasurementSchema measurementSchema =
            newFastCompactionPerformer
                .getReaderFromCache(firstChunkMetadataElement.fileElement.resource)
                .getMeasurementSchema(
                    Collections.singletonList(firstChunkMetadataElement.chunkMetadata));
        compactionWriter.startMeasurement(Collections.singletonList(measurementSchema), subTaskId);
        hasStartMeasurement = true;
      }

  }

  /**
   * Compact chunks in chunk metadata queue.
   *
   * @throws IOException
   * @throws PageException
   */
  private void compactChunks()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      List<ChunkMetadataElement> overlappedChunkMetadatas =
          findOverlapChunkMetadatas(firstChunkMetadataElement);
      boolean isChunkOverlap = overlappedChunkMetadatas.size() > 1;
      boolean isModified = isChunkModified(firstChunkMetadataElement);

      if (isChunkOverlap || isModified) {
        // has overlap or modified chunk, then deserialize it
        compactWithOverlapChunks(overlappedChunkMetadatas);
      } else {
        // has none overlap or modified chunk, flush it to file writer directly
        compactWithNonOverlapChunks(firstChunkMetadataElement);
      }
    }
  }

  /** Deserialize chunks and start compacting pages. */
  private void compactWithOverlapChunks(List<ChunkMetadataElement> overlappedChunkMetadatas)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    for (ChunkMetadataElement overlappedChunkMetadata : overlappedChunkMetadatas) {
      deserializeChunkIntoQueue(overlappedChunkMetadata);
    }
    compactPages();
  }

  /**
   * Flush chunk to target file directly. If the end time of chunk exceeds the end time of file,
   * then deserialize it.
   */
  private void compactWithNonOverlapChunks(ChunkMetadataElement chunkMetadataElement)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    if (compactionWriter.flushChunkToFileWriter(chunkMetadataElement.chunkMetadata, subTaskId)) {
      // flush chunk successfully
      removeChunk(chunkMetadataQueue.peek());
    } else {
      // chunk.endTime > file.endTime, then deserialize chunk
      compactWithOverlapChunks(Collections.singletonList(chunkMetadataElement));
    }
  }

  private void deserializeFileIntoQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException {
    int chunkMetadataPriority = 0;
    if (!isAligned) {
      for (FileElement fileElement : fileElements) {
        TsFileResource resource = fileElement.resource;
        Pair<Long, Long> timeseriesMetadataOffset =
            timeseriesMetadataOffsetMap.get(allMeasurements.get(currentSensorIndex)).get(resource);
        if (timeseriesMetadataOffset == null) {
          // tsfile does not contain this timeseries
          removeFile(fileElement);
          continue;
        }
        List<IChunkMetadata> iChunkMetadataList =
            newFastCompactionPerformer
                .getReaderFromCache(resource)
                .getChunkMetadataListByTimeseriesMetadataOffset(
                    timeseriesMetadataOffset.left, timeseriesMetadataOffset.right);

        if (iChunkMetadataList.size() > 0) {
          // modify chunk metadatas
          QueryUtils.modifyChunkMetaData(
              iChunkMetadataList,
              newFastCompactionPerformer.getModifications(
                  resource,
                  new PartialPath(deviceId, iChunkMetadataList.get(0).getMeasurementUid())));
          if (iChunkMetadataList.size() == 0) {
            // all chunks has been deleted in this file, just remove it
            removeFile(fileElement);
          }
        }

        for (int i = 0; i < iChunkMetadataList.size(); i++) {
          IChunkMetadata chunkMetadata = iChunkMetadataList.get(i);
          // set file path
          chunkMetadata.setFilePath(resource.getTsFilePath());

          // add into queue
          chunkMetadataQueue.add(
              new ChunkMetadataElement(
                  chunkMetadata,
                  (int) resource.getVersion(),
                  i == iChunkMetadataList.size() - 1,
                  fileElement));
        }
      }
    } else {
      for (FileElement fileElement : fileElements) {
        TsFileResource resource = fileElement.resource;
        TsFileSequenceReader reader = newFastCompactionPerformer.getReaderFromCache(resource);

        // read time chunk metadatas and value chunk metadatas in the current file
        List<IChunkMetadata> timeChunkMetadatas = new ArrayList<>();
        List<List<IChunkMetadata>> valueChunkMetadatas = new ArrayList<>();
        for (Map.Entry<String, Map<TsFileResource, Pair<Long, Long>>> entry :
            timeseriesMetadataOffsetMap.entrySet()) {
          String measurementID = entry.getKey();
          Pair<Long, Long> timeseriesOffsetInCurrentFile = entry.getValue().get(resource);
          if (measurementID.equals("")) {
            // read time chunk metadatas
            timeChunkMetadatas =
                reader.getChunkMetadataListByTimeseriesMetadataOffset(
                    timeseriesOffsetInCurrentFile.left, timeseriesOffsetInCurrentFile.right);
          } else {
            // read value chunk metadatas
            if (timeseriesOffsetInCurrentFile == null) {
              // current file does not contain this aligned timeseries
              valueChunkMetadatas.add(null);
            } else {
              // current file contains this aligned timeseries
              valueChunkMetadatas.add(
                  reader.getChunkMetadataListByTimeseriesMetadataOffset(
                      timeseriesOffsetInCurrentFile.left, timeseriesOffsetInCurrentFile.right));
            }
          }
        }

        // construct aligned chunk metadatas
        List<AlignedChunkMetadata> alignedChunkMetadataList = new ArrayList<>();
        for (int i = 0; i < timeChunkMetadatas.size(); i++) {
          List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
          for (List<IChunkMetadata> chunkMetadata : valueChunkMetadatas) {
            if (chunkMetadata == null) {
              valueChunkMetadataList.add(null);
            } else {
              valueChunkMetadataList.add(chunkMetadata.get(i));
            }
          }
          AlignedChunkMetadata alignedChunkMetadata =
              new AlignedChunkMetadata(timeChunkMetadatas.get(i), valueChunkMetadataList);

          // set file path
          alignedChunkMetadata.setFilePath(resource.getTsFilePath());
          alignedChunkMetadataList.add(alignedChunkMetadata);
        }

        // get value modifications of this file
        List<List<Modification>> valueModifications = new ArrayList<>();
        alignedChunkMetadataList
            .get(0)
            .getValueChunkMetadataList()
            .forEach(
                x -> {
                  try {
                    if (x == null) {
                      valueModifications.add(null);
                    } else {
                      valueModifications.add(
                          newFastCompactionPerformer.getModifications(
                              resource, new PartialPath(deviceId, x.getMeasurementUid())));
                    }
                  } catch (IllegalPathException e) {
                    throw new RuntimeException(e);
                  }
                });

        // modify aligned chunk metadatas
        QueryUtils.modifyAlignedChunkMetaData(alignedChunkMetadataList, valueModifications);

        if (alignedChunkMetadataList.size() == 0) {
          // all chunks has been deleted in this file, just remove it
          removeFile(fileElement);
        }

        // put aligned chunk metadatas into queue
        for (int i = 0; i < alignedChunkMetadataList.size(); i++) {
          chunkMetadataQueue.add(
              new ChunkMetadataElement(
                  alignedChunkMetadataList.get(i),
                  (int) resource.getVersion(),
                  i == alignedChunkMetadataList.size() - 1,
                  fileElement));
        }
      }

      //      for (FileElement fileElement : fileElements) {
      //        TsFileResource resource = fileElement.resource;
      //        List<ITimeSeriesMetadata> iTimeseriesMetadataList = new ArrayList<>();
      //        TsFileSequenceReader reader =
      // newFastCompactionPerformer.getReaderFromCache(resource);
      //        reader.getDeviceTimeseriesMetadata(
      //            iTimeseriesMetadataList,
      //            fileElement.firstMeasurementNode,
      //            Collections.emptySet(),
      //            true);
      //        AlignedTimeSeriesMetadata alignedTimeSeriesMetadata =
      //            (AlignedTimeSeriesMetadata) iTimeseriesMetadataList.get(0);
      //
      //
      //        // get value modifications of this file
      //        List<List<Modification>> valueModifications = new ArrayList<>();
      //        alignedTimeSeriesMetadata
      //            .getValueTimeseriesMetadataList()
      //            .forEach(
      //                x -> {
      //                  try {
      //                    valueModifications.add(
      //                        newFastCompactionPerformer.getModifications(
      //                            resource, new PartialPath(deviceId, x.getMeasurementId())));
      //                  } catch (IllegalPathException e) {
      //                    throw new RuntimeException(e);
      //                  }
      //                });
      //
      //        List<AlignedChunkMetadata> alignedChunkMetadataList =
      //            alignedTimeSeriesMetadata.getChunkMetadataList();
      //        // modify aligned chunk metadatas
      //        QueryUtils.modifyAlignedChunkMetaData(alignedChunkMetadataList, valueModifications);
      //
      //        if (alignedChunkMetadataList.size() == 0) {
      //          // all chunks has been deleted in this file, just remove it
      //          removeFile(fileElement);
      //        }
      //
      //        Map<String, IChunkMetadata> valueChunkMetadataMap = new LinkedHashMap<>();
      //        for (int i = 0; i < alignedChunkMetadataList.size(); i++) {
      //          AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(i);
      //          List<IChunkMetadata> valueChunkMetadataList =
      //              alignedChunkMetadata.getValueChunkMetadataList();
      //          int valueIndex = 0;
      //          for (int i = 0; i < allMeasruements.size(); i++) {
      //            if (allMeasruements.get(i) <= valueChunkMetadataList.get(valueIndex))
      //              valueChunkMetadataList.add();
      //          }
      //          alignedChunkMetadata
      //              .getValueChunkMetadataList()
      //              .forEach(
      //                  x -> {
      //                    if (!measurementSchemaMap.containsKey(x.getMeasurementUid())) {
      //                      try {
      //                        measurementSchemaMap.put(
      //                            x.getMeasurementUid(),
      //                            reader.getMeasurementSchema(Collections.singletonList(x)));
      //                      } catch (IOException e) {
      //                        throw new RuntimeException(e);
      //                      }
      //                    }
      //                  });
      //          // set file path
      //          alignedChunkMetadata.setFilePath(resource.getTsFilePath());
      //
      //          // add into queue
      //          chunkMetadataQueue.add(
      //              new ChunkMetadataElement(
      //                  alignedChunkMetadata,
      //                  (int) resource.getVersion(),
      //                  i == alignedChunkMetadataList.size() - 1,
      //                  fileElement));
      //        }
      //      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  private void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    if (chunkMetadataElement.chunkMetadata instanceof ChunkMetadata) {
      Chunk chunk =
          ChunkCache.getInstance().get((ChunkMetadata) chunkMetadataElement.chunkMetadata);
      ChunkReader chunkReader = new ChunkReader(chunk);
      ByteBuffer chunkDataBuffer = chunk.getData();
      ChunkHeader chunkHeader = chunk.getHeader();
      while (chunkDataBuffer.remaining() > 0) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }
        ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

        boolean isLastPage = chunkDataBuffer.remaining() <= 0;
        pageQueue.add(
            new PageElement(
                pageHeader,
                compressedPageData,
                chunkReader,
                chunkMetadataElement,
                isLastPage,
                chunkMetadataElement.priority));
      }
    } else {
      AlignedChunkMetadata alignedChunkMetadata =
          (AlignedChunkMetadata) chunkMetadataElement.chunkMetadata;

      List<PageHeader> timePageHeaders = new ArrayList<>();
      List<ByteBuffer> compressedTimePageDatas = new ArrayList<>();
      List<List<PageHeader>> valuePageHeaders = new ArrayList<>();
      List<List<ByteBuffer>> compressedValuePageDatas = new ArrayList<>();

      // deserialize time chunk
      ChunkMetadata chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
      Chunk timeChunk = ChunkCache.getInstance().get(chunkMetadata);
      ChunkReader chunkReader = new ChunkReader(timeChunk);
      ByteBuffer chunkDataBuffer = timeChunk.getData();
      ChunkHeader chunkHeader = timeChunk.getHeader();
      while (chunkDataBuffer.remaining() > 0) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, timeChunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }
        ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);
        timePageHeaders.add(pageHeader);
        compressedTimePageDatas.add(compressedPageData);
      }

      // deserialize value chunks
      List<Chunk> valueChunks = new ArrayList<>(measurementSchemaMap.size());
      for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
        chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getValueChunkMetadataList().get(i);
        if (chunkMetadata == null) {
          // value chunk has been deleted completely
          valuePageHeaders.add(null);
          compressedValuePageDatas.add(null);
          valueChunks.add(null);
          continue;
        }
        Chunk valueChunk = ChunkCache.getInstance().get(chunkMetadata);
        chunkReader = new ChunkReader(valueChunk);
        chunkDataBuffer = valueChunk.getData();
        chunkHeader = valueChunk.getHeader();
        valueChunks.add(valueChunk);

        valuePageHeaders.add(new ArrayList<>());
        compressedValuePageDatas.add(new ArrayList<>());
        while (chunkDataBuffer.remaining() > 0) {
          // deserialize a PageHeader from chunkDataBuffer

          PageHeader pageHeader;
          if (((byte) (chunkHeader.getChunkType() & 0x3F))
              == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
            pageHeader =
                PageHeader.deserializeFrom(chunkDataBuffer, valueChunk.getChunkStatistic());
          } else {
            pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
          }
          ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);
          valuePageHeaders.get(i).add(pageHeader);
          compressedValuePageDatas.get(i).add(compressedPageData);
        }
      }

      // add aligned pages into page queue
      for (int i = 0; i < timePageHeaders.size(); i++) {
        List<PageHeader> alignedPageHeaders = new ArrayList<>();
        List<ByteBuffer> alignedPageDatas = new ArrayList<>();
        for (int j = 0; j < valuePageHeaders.size(); j++) {
          if (valuePageHeaders.get(j) == null) {
            alignedPageHeaders.add(null);
            alignedPageDatas.add(null);
            continue;
          }
          alignedPageHeaders.add(valuePageHeaders.get(j).get(i));
          alignedPageDatas.add(compressedValuePageDatas.get(j).get(i));
        }
        pageQueue.add(
            new PageElement(
                timePageHeaders.get(i),
                alignedPageHeaders,
                compressedTimePageDatas.get(i),
                alignedPageDatas,
                new AlignedChunkReader(timeChunk, valueChunks, null),
                chunkMetadataElement,
                i == timePageHeaders.size() - 1,
                chunkMetadataElement.priority));
      }

      //      // deserialize value chunks
      //      List<Chunk> valueChunks = new ArrayList<>();
      //      for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
      //        chunkMetadata = (ChunkMetadata)
      // alignedChunkMetadata.getValueChunkMetadataList().get(i);
      //        if (chunkMetadata == null) {
      //          // value chunk has been deleted completely
      //          valuePageHeaders.;
      //          compressedValuePageDatas.add(null);
      //          continue;
      //        }
      //        Chunk valueChunk = ChunkCache.getInstance().get(chunkMetadata);
      //        chunkReader = new ChunkReader(valueChunk);
      //        chunkDataBuffer = valueChunk.getData();
      //        chunkHeader = valueChunk.getHeader();
      //        valueChunks.add(valueChunk);
      //        int pageIndex = 0;
      //        while (chunkDataBuffer.remaining() > 0) {
      //          // deserialize a PageHeader from chunkDataBuffer
      //          if (valuePageHeaders.size() <= pageIndex) {
      //            valuePageHeaders.add(new ArrayList<>());
      //          }
      //          if (compressedValuePageDatas.size() <= pageIndex) {
      //            compressedValuePageDatas.add(new ArrayList<>());
      //          }
      //          PageHeader pageHeader;
      //          if (((byte) (chunkHeader.getChunkType() & 0x3F))
      //              == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      //            pageHeader =
      //                PageHeader.deserializeFrom(chunkDataBuffer, valueChunk.getChunkStatistic());
      //          } else {
      //            pageHeader = PageHeader.deserializeFrom(chunkDataBuffer,
      // chunkHeader.getDataType());
      //          }
      //          ByteBuffer compressedPageData =
      // chunkReader.readPageDataWithoutUncompressing(pageHeader);
      //          valuePageHeaders.get(pageIndex).add(pageHeader);
      //          compressedValuePageDatas.get(pageIndex++).add(compressedPageData);
      //        }
      //      }
      //
      //      // add aligned pages into page queue
      //      for (int i = 0; i < timePageHeaders.size(); i++) {
      //        pageQueue.add(
      //            new PageElement(
      //                timePageHeaders.get(i),
      //                valuePageHeaders.get(i),
      //                compressedTimePageDatas.get(i),
      //                compressedValuePageDatas.get(i),
      //                new AlignedChunkReader(timeChunk, valueChunks, null),
      //                chunkMetadataElement,
      //                i == timePageHeaders.size() - 1,
      //                chunkMetadataElement.priority));
      //      }
    }
  }

  /**
   * Compact pages in page queue.
   *
   * @throws IOException
   * @throws PageException
   */
  private void compactPages()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      List<PageElement> overlappedPages = findOverlapPages(firstPageElement);
      boolean isPageOverlap = overlappedPages.size() > 1;
      int modifiedStatus = isPageModified(firstPageElement);

      switch (modifiedStatus) {
        case -1:
          // no data on this page has been deleted
          if (isPageOverlap) {
            // has overlap pages, deserialize it
            compactWithOverlapPages(overlappedPages);
          } else {
            // has none overlap pages,  flush it to chunk writer directly
            compactWithNonOverlapPage(firstPageElement);
          }
          break;
        case 0:
          // there is data on this page been deleted, deserialize it
          compactWithOverlapPages(overlappedPages);
          break;
        case 1:
          // all data on this page has been deleted, remove it
          removePage(firstPageElement, null);
          break;
      }
    }
  }

  private void compactWithNonOverlapPage(PageElement pageElement)
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    boolean success;
    if (pageElement.iChunkReader instanceof AlignedChunkReader) {
      success =
          compactionWriter.flushAlignedPageToChunkWriter(
              pageElement.pageData,
              pageElement.pageHeader,
              pageElement.valuePageDatas,
              pageElement.valuePageHeaders,
              subTaskId);
    } else {
      success =
          compactionWriter.flushPageToChunkWriter(
              pageElement.pageData, pageElement.pageHeader, subTaskId);
    }
    if (success) {
      // flush the page successfully
      removePage(pageElement, null);
    } else {
      // page.endTime > file.endTime, then deserialze it
      List<PageElement> pageElements = new ArrayList<>();
      pageElements.add(pageElement);
      compactWithOverlapPages(pageElements);
    }
  }

  /**
   * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
   * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
   * with page 4,and so on, there are 10 pages in total. This method will merge all 10 pages.
   */
  private void compactWithOverlapPages(List<PageElement> overlappedPages)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    pointPriorityReader.addNewPage(overlappedPages.remove(0));
    pointPriorityReader.setNewOverlappedPages(overlappedPages);
    while (pointPriorityReader.hasNext()) {
      for (int pageIndex = 0; pageIndex < overlappedPages.size(); pageIndex++) {
        PageElement nextPageElement = overlappedPages.get(pageIndex);

        // write currentPage.point.time < nextPage.startTime to chunk writer
        while (pointPriorityReader.currentPoint().left < nextPageElement.startTime) {
          // write data point to chunk writer
          compactionWriter.write(
              pointPriorityReader.currentPoint().left,
              pointPriorityReader.currentPoint().right,
              subTaskId);
          pointPriorityReader.next();
        }

        boolean isNextPageOverlap =
            !(pointPriorityReader.currentPoint().left > nextPageElement.pageHeader.getEndTime()
                && !isPageOverlap(nextPageElement));

        switch (isPageModified(nextPageElement)) {
          case -1:
            // no data on this page has been deleted
            if (isNextPageOverlap) {
              // has overlap with other pages, deserialize it
              pointPriorityReader.addNewPage(nextPageElement);
            } else {
              // has none overlap, flush it to chunk writer directly
              compactWithNonOverlapPage(nextPageElement);
            }
            break;
          case 0:
            // there is data on this page been deleted, deserialize it
            pointPriorityReader.addNewPage(nextPageElement);
            break;
          case 1:
            // all data on this page has been deleted, remove it
            removePage(nextPageElement, null);
            break;
        }
      }

      overlappedPages.clear();

      // write remaining data points
      while (pointPriorityReader.hasNext()) {
        // write data point to chunk writer
        compactionWriter.write(
            pointPriorityReader.currentPoint().left,
            pointPriorityReader.currentPoint().right,
            subTaskId);
        pointPriorityReader.next();
        if (overlappedPages.size() > 0) {
          // finish compacting the first page and find the new overlapped pages, then start
          // compacting them with new first page
          break;
        }
      }
    }
  }

  /** Find overlaped pages which is not been selected with the specific page. */
  private List<PageElement> findOverlapPages(PageElement page) {
    List<PageElement> elements = new ArrayList<>();
    long endTime = page.pageHeader.getEndTime();
    Stream<PageElement> pages =
        pageQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<PageElement> it = pages.iterator();
    while (it.hasNext()) {
      PageElement element = it.next();
      if (element.startTime <= endTime) {
        if (!element.isOverlaped) {
          elements.add(element);
          element.isOverlaped = true;
        }
      } else {
        break;
      }
    }
    return elements;
  }

  /** Find overlaped chunks which is not been selected with the specific chunk. */
  private List<ChunkMetadataElement> findOverlapChunkMetadatas(ChunkMetadataElement chunkMetadata) {
    List<ChunkMetadataElement> elements = new ArrayList<>();
    long endTime = chunkMetadata.chunkMetadata.getEndTime();
    Stream<ChunkMetadataElement> chunks =
        chunkMetadataQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<ChunkMetadataElement> it = chunks.iterator();
    while (it.hasNext()) {
      ChunkMetadataElement element = it.next();
      if (element.chunkMetadata.getStartTime() <= endTime) {
        if (!element.isOverlaped) {
          elements.add(element);
          element.isOverlaped = true;
        }
      } else {
        break;
      }
    }
    return elements;
  }

  private List<FileElement> findOverlapFiles(FileElement file) {
    List<FileElement> overlappedFiles = new ArrayList<>();
    long endTime = file.resource.getEndTime(deviceId);
    for (FileElement fileElement : fileList) {
      if (fileElement.resource.getStartTime(deviceId) <= endTime) {
        if (!fileElement.isOverlap) {
          overlappedFiles.add(fileElement);
          fileElement.isOverlap = true;
        }
      } else {
        break;
      }
    }
    return overlappedFiles;
  }

  private boolean isChunkOverlap(ChunkMetadataElement chunkMetadataElement) {
    return false;
  }

  /** Check is the page overlap with other pages later then the specific page in queue or not. */
  private boolean isPageOverlap(PageElement pageElement) {
    long endTime = pageElement.pageHeader.getEndTime();
    Stream<PageElement> pages =
        pageQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<PageElement> it = pages.iterator();
    while (it.hasNext()) {
      PageElement element = it.next();
      // only check pages later then the specific page
      if (element.equals(pageElement) || element.startTime < pageElement.startTime) {
        continue;
      }
      return element.startTime <= endTime;
    }
    return false;
  }

  /**
   * Check whether the chunk is modified.
   *
   * <p>Notice: if is aligned chunk, return true if any of value chunk has data been deleted. Return
   * false if and only if all value chunks has no data been deleted.
   */
  private boolean isChunkModified(ChunkMetadataElement chunkMetadataElement) {
    if (isAligned) {
      AlignedChunkMetadata alignedChunkMetadata =
          (AlignedChunkMetadata) chunkMetadataElement.chunkMetadata;
      boolean isAlignedChunkModified = alignedChunkMetadata.isModified();
      if (!isAlignedChunkModified) {
        // check if one of the value chunk has been deleted completely
        isAlignedChunkModified = alignedChunkMetadata.getValueChunkMetadataList().contains(null);
      }
      return isAlignedChunkModified;
    } else {
      return chunkMetadataElement.chunkMetadata.isModified();
    }
  }

  /**
   * -1 means that no data on this page has been deleted. <br>
   * 0 means that there is data on this page been deleted. <br>
   * 1 means that all data on this page has been deleted.
   *
   * <p>Notice: If is aligned page, return 1 if and only if all value pages are deleted. Return -1
   * if and only if no data exists on all value pages is deleted
   */
  private int isPageModified(PageElement pageElement) {
    long startTime = pageElement.startTime;
    long endTime = pageElement.pageHeader.getEndTime();
    if (isAligned) {
      AlignedChunkMetadata alignedChunkMetadata =
          (AlignedChunkMetadata) pageElement.chunkMetadataElement.chunkMetadata;
      int lastPageStatus = Integer.MIN_VALUE;
      for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        int currentPageStatus =
            valueChunkMetadata == null
                ? 1
                : checkIsModified(startTime, endTime, valueChunkMetadata.getDeleteIntervalList());
        if (currentPageStatus == 0) {
          return 0;
        }
        if (lastPageStatus == Integer.MIN_VALUE) {
          // first page
          lastPageStatus = currentPageStatus;
          continue;
        }
        if (currentPageStatus != lastPageStatus) {
          return 0;
        }
      }
      return lastPageStatus;
    } else {
      return checkIsModified(
          startTime,
          endTime,
          pageElement.chunkMetadataElement.chunkMetadata.getDeleteIntervalList());
    }
  }

  private int checkIsModified(long startTime, long endTime, Collection<TimeRange> deletions) {
    int status = -1;
    if (deletions != null) {
      for (TimeRange range : deletions) {
        if (range.contains(startTime, endTime)) {
          // all data on this page has been deleted
          return 1;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          // exist data on this page been deleted
          status = 0;
        }
      }
    }
    return status;
  }

  private void removePage(PageElement pageElement, List<PageElement> newOverlappedPages)
      throws IOException, IllegalPathException {
    // check is first page or not
    boolean isFirstPage = pageQueue.peek().equals(pageElement);
    pageQueue.remove(pageElement);
    if (pageElement.isLastPage) {
      // finish compacting the chunk, remove it from queue
      removeChunk(pageElement.chunkMetadataElement);
    }
    if (newOverlappedPages == null) {
      // uncessary to find new overlap pages with current top page, just return
      return;
    }
    if (pointPriorityReader.hasNext() && pageQueue.size() != 0) {
      // pointPriorityReader.hasNext() indicates that the new first page in page queue has not been
      // finished compacting yet, so there may be other pages overlap with it.
      // when deserializing new chunks into page queue or first page is removed from page queue, we
      // should find new overlapped pages and put them into list
      newOverlappedPages.addAll(findOverlapPages(pageQueue.peek()));
    }
  }

  private void removeChunk(ChunkMetadataElement chunkMetadataElement)
      throws IOException, IllegalPathException {

    chunkMetadataQueue.remove(chunkMetadataElement);
    if (chunkMetadataElement.isLastChunk) {
      // finish compacting the file, remove it from list
      removeFile(chunkMetadataElement.fileElement);
    }

    if (pageQueue.size() != 0 && chunkMetadataQueue.size() != 0) {
      // pageQueue.size > 0 indicates that the new first chunk in chunk metadata queue has not been
      // finished compacting yet, so there may be other chunks overlap with it.
      // when deserializing new files into chunk metadata queue or first chunk is removed from chunk
      // metadata queue, we should find new overlapped chunks and deserialize them into page queue
      for (ChunkMetadataElement newOverlappedChunkMetadata :
          findOverlapChunkMetadatas(chunkMetadataQueue.peek())) {
        deserializeChunkIntoQueue(newOverlappedChunkMetadata);
      }
    }

    //    // Todo?
    //    // check is first chunk or not
    //    boolean isFirstChunk = chunkMetadataQueue.peek().equals(chunkMetadataElement);
    //    if (isFirstChunk && !chunkMetadataQueue.isEmpty()) {
    //      if (!pageQueue.isEmpty()) {
    //        // find new overlapped chunks and deserialize them into page queue
    //        for (ChunkMetadataElement newOverlappedChunkMetadata :
    //            findOverlapChunkMetadatas(chunkMetadataQueue.peek())) {
    //          deserializeChunkIntoQueue(newOverlappedChunkMetadata);
    //        }
    //      }
    //    }
  }

  private void removeFile(FileElement fileElement) throws IllegalPathException, IOException {
    boolean isFirstFile = fileList.get(0).equals(fileElement);
    fileList.remove(fileElement);
    if (isFirstFile && !fileList.isEmpty()) {
      // find new overlapped files and deserialize them into chunk metadata queue
      deserializeFileIntoQueue(findOverlapFiles(fileList.get(0)));
    }
  }

  private List<Collection<TimeRange>> getAlignedChunkDeletions(
      AlignedChunkMetadata alignedChunkMetadata) {
    List<Collection<TimeRange>> deletions = new ArrayList<>();
    for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      Collection<TimeRange> valueDeletions = valueChunkMetadata.getDeleteIntervalList();
      deletions.add(valueDeletions);
    }
    return deletions;
  }
}
