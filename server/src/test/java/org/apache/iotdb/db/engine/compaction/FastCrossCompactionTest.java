package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.junit.Assert.fail;

public class FastCrossCompactionTest extends AbstractCompactionTest {

  private int fileVersion = 0;
  private final String testStorageGroup = "root.testsg";

  TsFileManager tsFileManager =
      new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  @Test
  public void test1() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < 10; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(10);

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter :
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 599);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < 15; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(5);

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(200L, 2200L));

        for (IChunkWriter iChunkWriter :
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 200);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2199);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // seq file 2
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < 12; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(5);

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(900L, 1400L));
        pages.add(new TimeRange(1550L, 1700L));
        pages.add(new TimeRange(1750L, 2000L));

        for (IChunkWriter iChunkWriter :
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 900);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1999);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 2
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < 15; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(5);

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1600L));

        for (IChunkWriter iChunkWriter :
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1599);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test2() throws MetadataException, IOException {
    IoTDBDescriptor.getInstance().getConfig().setChunkPointNumLowerBoundInCompaction(1000);
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(100L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter :
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 10;
    measurementNum = 10;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1189L));
        timeRanges.add(new TimeRange(1301L, 1400L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1800L, 1900L));
        timeRanges.add(new TimeRange(2150L, 2250L));
        timeRanges.add(new TimeRange(2300L, 2500L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 900);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 12;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(0L, 1189L));
        timeRanges.add(new TimeRange(1301L, 2000L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2100L, 2200L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(550L, 800L));
        timeRanges.add(new TimeRange(1200L, 1300L));
        timeRanges.add(new TimeRange(1500L, 2200L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 5;
    measurementNum = 7;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1150L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test3() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(100L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter :
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 10;
    measurementNum = 10;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1249L));
        timeRanges.add(new TimeRange(1351L, 1400L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1800L, 1900L));
        timeRanges.add(new TimeRange(2150L, 2250L));
        timeRanges.add(new TimeRange(2300L, 2500L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 900);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 12;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(0L, 1249L));
        timeRanges.add(new TimeRange(1351L, 2000L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2100L, 2200L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(550L, 800L));
        timeRanges.add(new TimeRange(1250L, 1350L));
        timeRanges.add(new TimeRange(1500L, 2200L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 5;
    measurementNum = 7;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1150L));

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test4() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 5;
    measurementNum = 7;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2700, 2800));
        timeRanges.add(new TimeRange(2900, 3000));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test5() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2630, 2680));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(4000, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2630);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(2900, 3000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3800, 3900));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3900);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 5;
    measurementNum = 7;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(3100, 3200));
        timeRanges.add(new TimeRange(3300, 3400));
        timeRanges.add(new TimeRange(3450, 3550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3550);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test6() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2620L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2620);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(4000, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(2900, 3000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3800, 3900));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3900);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 5;
    measurementNum = 7;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2630, 2690));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write third chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(3100, 3200));
        timeRanges.add(new TimeRange(3300, 3400));
        timeRanges.add(new TimeRange(3450, 3550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3550);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test7() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2, time range of d0.s3~s19 and d1.s3~s19 is 1800 ~ 2650, others is 1800 ~ 2620,
    // which may cause chunk 18 of d0.s0~s2 and d1.s0~s2 will be deserialized, although it is not
    // overlapped with others.
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2620L));

        for (int i = 0; i < iChunkWriters.size(); i++) {
          if (deviceIndex < 2 && i == 3) {
            pages.add(new TimeRange(2621, 2650));
          }
          IChunkWriter iChunkWriter = iChunkWriters.get(i);
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        if (deviceIndex < 2) {
          resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2650);
        } else {
          resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2620);
        }
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(4000, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(2900, 3000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3800, 3900));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3900);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 5;
    measurementNum = 7;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2630, 2690));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write third chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(3100, 3200));
        timeRanges.add(new TimeRange(3300, 3400));
        timeRanges.add(new TimeRange(3450, 3550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3550);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test8() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2630, 2680));
        pages.add(new TimeRange(2850, 2950));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2630);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(3000, 3100));
        timeRanges.add(new TimeRange(3450, 3550));
        timeRanges.add(new TimeRange(3750, 3850));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3850);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test9() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2620, 2670));
        timeRanges.add(new TimeRange(3000, 3050));
        timeRanges.add(new TimeRange(3450, 3550));
        timeRanges.add(new TimeRange(3750, 3850));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3850);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test10() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2, time range of d0.s3~s19 and d1.s3~s19 is 1800 ~ 2650, others is 1800 ~ 2500,
    // which may cause page 18 of d0.s0~s2 and d1.s0~s2 will be deserialized, although it is not
    // overlapped with others.
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500));

        for (int i = 0; i < iChunkWriters.size(); i++) {
          if (deviceIndex < 2 && i == 3) {
            pages.add(new TimeRange(2501, 2650));
          }
          IChunkWriter iChunkWriter = iChunkWriters.get(i);
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        if (deviceIndex < 2) {
          resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2650);
        } else {
          resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        }
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2620, 2670));
        timeRanges.add(new TimeRange(3000, 3050));
        timeRanges.add(new TimeRange(3450, 3550));
        timeRanges.add(new TimeRange(3750, 3850));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 3850);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test11() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1840, 1900);
        generateModsFile(timeseriesPath, resource, 2150, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2801, 2850);
        generateModsFile(timeseriesPath, resource, 3950, 4100);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1149L));
        timeRanges.add(new TimeRange(1351L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 300, 500);
        generateModsFile(timeseriesPath, resource, 1450, 1650);
        generateModsFile(timeseriesPath, resource, 1700, 1790);
        generateModsFile(timeseriesPath, resource, 1830, 2000);
        generateModsFile(timeseriesPath, resource, 2200, 2260);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2700, 2800);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1850, 2000));
        timeRanges.add(new TimeRange(2100, 2230));
        timeRanges.add(new TimeRange(2240, 2300));
        timeRanges.add(new TimeRange(2399, 2550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1160, 1250);
        generateModsFile(timeseriesPath, resource, 1850, 2000);
        generateModsFile(timeseriesPath, resource, 2100, 2230);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 5
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1420L, 1800));
        pages.add(new TimeRange(1880, 2250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1200, 1300);
        generateModsFile(timeseriesPath, resource, 1450, 1780);
        generateModsFile(timeseriesPath, resource, 1880, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test12() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1840, 1900);
        generateModsFile(timeseriesPath, resource, 2150, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2801, 2850);
        generateModsFile(timeseriesPath, resource, 3950, 4100);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1149L));
        timeRanges.add(new TimeRange(1351L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 300, 500);
        generateModsFile(timeseriesPath, resource, 1450, 1650);
        generateModsFile(timeseriesPath, resource, 1700, 1790);
        generateModsFile(timeseriesPath, resource, 1830, 2000);
        generateModsFile(timeseriesPath, resource, 2200, 2260);
        generateModsFile(timeseriesPath, resource, 1700, 2000);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2700, 2800);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1850, 2000));
        timeRanges.add(new TimeRange(2100, 2230));
        timeRanges.add(new TimeRange(2240, 2300));
        timeRanges.add(new TimeRange(2399, 2550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1160, 1250);
        generateModsFile(timeseriesPath, resource, 1850, 2000);
        generateModsFile(timeseriesPath, resource, 2100, 2230);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 5
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1420L, 1800));
        pages.add(new TimeRange(1880, 2250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1200, 1300);
        generateModsFile(timeseriesPath, resource, 1450, 1780);
        generateModsFile(timeseriesPath, resource, 1880, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test13() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1840, 1900);
        generateModsFile(timeseriesPath, resource, 2150, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2801, 2850);
        generateModsFile(timeseriesPath, resource, 3950, 4100);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1149L));
        timeRanges.add(new TimeRange(1351L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 300, 500);
        generateModsFile(timeseriesPath, resource, 1450, 1650);
        generateModsFile(timeseriesPath, resource, 1700, 1790);
        generateModsFile(timeseriesPath, resource, 1830, 2000);
        generateModsFile(timeseriesPath, resource, 2200, 2260);
        generateModsFile(timeseriesPath, resource, 1700, 2000);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2700, 2800);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1850, 2000));
        timeRanges.add(new TimeRange(2100, 2230));
        timeRanges.add(new TimeRange(2240, 2300));
        timeRanges.add(new TimeRange(2399, 2550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1160, 1250);
        generateModsFile(timeseriesPath, resource, 1850, 2000);
        generateModsFile(timeseriesPath, resource, 2100, 2230);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 5
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1420L, 1800));
        pages.add(new TimeRange(1880, 2250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1200, 1300);
        generateModsFile(timeseriesPath, resource, 1420, 1800);
        generateModsFile(timeseriesPath, resource, 1880, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test14() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1840, 1900);
        generateModsFile(timeseriesPath, resource, 2150, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2801, 2850);
        generateModsFile(timeseriesPath, resource, 3950, 4100);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1149L));
        timeRanges.add(new TimeRange(1351L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 300, 500);
        generateModsFile(timeseriesPath, resource, 1450, 1650);
        generateModsFile(timeseriesPath, resource, 1700, 1790);
        generateModsFile(timeseriesPath, resource, 1830, 2000);
        generateModsFile(timeseriesPath, resource, 2200, 2260);
        generateModsFile(timeseriesPath, resource, 1700, 2000);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2700, 2800);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1850, 2000));
        timeRanges.add(new TimeRange(2100, 2230));
        timeRanges.add(new TimeRange(2240, 2300));
        timeRanges.add(new TimeRange(2399, 2550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1160, 1250);
        generateModsFile(timeseriesPath, resource, 1850, 2000);
        generateModsFile(timeseriesPath, resource, 2100, 2230);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 5
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1420L, 1800));
        pages.add(new TimeRange(1880, 2250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1200, 1300);
        generateModsFile(timeseriesPath, resource, 1420, 1800);
        generateModsFile(timeseriesPath, resource, 1880, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 6
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2800, 4010);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void test15() throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);

        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, true);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 0);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 1800);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1840, 1900);
        generateModsFile(timeseriesPath, resource, 2150, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2801);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2801, 2850);
        generateModsFile(timeseriesPath, resource, 3950, 4100);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // unseq file 1
    deviceNum = 20;
    measurementNum = 10;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(100L, 1149L));
        timeRanges.add(new TimeRange(1351L, 1650L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneNonAlignedPage((ChunkWriterImpl) iChunkWriter, timeRanges, false);
          writeOneNonAlignedPage(
              (ChunkWriterImpl) iChunkWriter,
              Collections.singletonList(new TimeRange(1700, 2000)),
              false);
          ((ChunkWriterImpl) iChunkWriter).writeToFileWriter(tsFileIOWriter);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(2200, 2400));
        timeRanges.add(new TimeRange(2500, 2600));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 100);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 300, 500);
        generateModsFile(timeseriesPath, resource, 1450, 1650);
        generateModsFile(timeseriesPath, resource, 2200, 2260);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 2
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2700, 2800));
        pages.add(new TimeRange(3150, 3250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3600, 3700));
        pages.add(new TimeRange(3900, 4000));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2700);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 4000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2700, 2800);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 3
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1500L, 1750L));
        pages.add(new TimeRange(1850L, 2200));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2200);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 4
    deviceNum = 20;
    measurementNum = 20;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(350L, 400L));
        timeRanges.add(new TimeRange(550L, 700L));
        timeRanges.add(new TimeRange(1050L, 1250L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        // write second chunk
        timeRanges.clear();
        timeRanges.add(new TimeRange(1850, 2000));
        timeRanges.add(new TimeRange(2100, 2230));
        timeRanges.add(new TimeRange(2240, 2300));
        timeRanges.add(new TimeRange(2399, 2550));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, timeRanges, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 350);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1160, 1250);
        generateModsFile(timeseriesPath, resource, 1850, 2000);
        generateModsFile(timeseriesPath, resource, 2100, 2230);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    // unseq file 5
    deviceNum = 12;
    measurementNum = 15;
    resource = createEmptyFileAndResource(false);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex);

        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new PartialPath(
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i));
        }
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<IChunkWriter> iChunkWriters =
            registerTimeseriesAndCreateChunkWriter(timeseriesPath, dataTypes, false);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(550L, 800L));
        pages.add(new TimeRange(1200L, 1300L));
        pages.add(new TimeRange(1420L, 1800));
        pages.add(new TimeRange(1880, 2250));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, false);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 550);
        resource.updateEndTime(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex, 2250);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1200, 1300);
        generateModsFile(timeseriesPath, resource, 1420, 1800);
        generateModsFile(timeseriesPath, resource, 1880, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    unseqResources.add(resource);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(timeserisPathList, tsDataTypes);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    validateSeqFiles();

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  public List<IChunkWriter> registerTimeseriesAndCreateChunkWriter(
      List<PartialPath> timeseriesPaths, List<TSDataType> dataTypes, boolean isAligned)
      throws MetadataException {
    List<IChunkWriter> iChunkWriters = new ArrayList<>();
    if (!isAligned) {
      for (int i = 0; i < timeseriesPaths.size(); i++) {
        PartialPath path = timeseriesPaths.get(i);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            dataTypes.get(i),
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            Collections.emptyMap());
        MeasurementSchema schema = new MeasurementSchema(path.getMeasurement(), dataTypes.get(i));
        iChunkWriters.add(new ChunkWriterImpl(schema));
      }
    } else {
      List<IMeasurementSchema> schemas = new ArrayList<>();
      for (int i = 0; i < timeseriesPaths.size(); i++) {
        schemas.add(
            new MeasurementSchema(timeseriesPaths.get(i).getMeasurement(), dataTypes.get(i)));
      }
      iChunkWriters.add(new AlignedChunkWriterImpl(schemas));
    }

    return iChunkWriters;
  }

  private void writeOneNonAlignedPage(
      ChunkWriterImpl chunkWriter, List<TimeRange> timeRanges, boolean isSeq) {
    PageWriter pageWriter = chunkWriter.getPageWriter();
    for (TimeRange timeRange : timeRanges) {
      for (long timestamp = timeRange.getMin(); timestamp <= timeRange.getMax(); timestamp++) {
        switch (chunkWriter.getDataType()) {
          case TEXT:
            pageWriter.write(timestamp, new Binary(isSeq ? "seqText" : "unSeqText"));
            break;
          case DOUBLE:
            pageWriter.write(timestamp, isSeq ? timestamp + 0.01 : 100000.01 + timestamp);
            break;
          case BOOLEAN:
            pageWriter.write(timestamp, isSeq);
            break;
          case INT64:
            pageWriter.write(timestamp, isSeq ? timestamp : 100000L + timestamp);
            break;
          case INT32:
            pageWriter.write(timestamp, isSeq ? (int) timestamp : (int) (100000 + timestamp));
            break;
          case FLOAT:
            pageWriter.write(
                timestamp, isSeq ? (float) (timestamp + 0.1) : (float) (100000.1 + timestamp));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown data type " + chunkWriter.getDataType());
        }
      }
    }
    chunkWriter.sealCurrentPage();
  }

  private void writeNonAlignedChunk(
      ChunkWriterImpl chunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<TimeRange> pages,
      boolean isSeq)
      throws IOException {
    PageWriter pageWriter = chunkWriter.getPageWriter();
    for (TimeRange page : pages) {
      // write a page
      for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
        switch (chunkWriter.getDataType()) {
          case TEXT:
            pageWriter.write(timestamp, new Binary(isSeq ? "seqText" : "unSeqText"));
            break;
          case DOUBLE:
            pageWriter.write(timestamp, isSeq ? timestamp + 0.01 : 100000.01 + timestamp);
            break;
          case BOOLEAN:
            pageWriter.write(timestamp, isSeq);
            break;
          case INT64:
            pageWriter.write(timestamp, isSeq ? timestamp : 100000L + timestamp);
            break;
          case INT32:
            pageWriter.write(timestamp, isSeq ? (int) timestamp : (int) (100000 + timestamp));
            break;
          case FLOAT:
            pageWriter.write(
                timestamp, isSeq ? (float) (timestamp + 0.1) : (float) (100000.1 + timestamp));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown data type " + chunkWriter.getDataType());
        }
      }
      // seal the current page
      chunkWriter.sealCurrentPage();
    }
    // seal current chunk
    chunkWriter.writeToFileWriter(tsFileIOWriter);
  }

  private void writeAlignedChunk(
      AlignedChunkWriterImpl alignedChunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<TimeRange> pages,
      boolean isSeq,
      List<Integer> subSensorIndexList)
      throws IOException {
    TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
    for (TimeRange page : pages) {
      // write time page
      for (long timestamp = page.getMin(); timestamp < page.getMax(); timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      // write value page
      for (Integer valueIndex : subSensorIndexList) {
        ValueChunkWriter valueChunkWriter =
            alignedChunkWriter.getValueChunkWriterByIndex(valueIndex);
        ValuePageWriter valuePageWriter = valueChunkWriter.getPageWriter();
        for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
          switch (valueChunkWriter.getDataType()) {
            case TEXT:
              valuePageWriter.write(timestamp, new Binary(isSeq ? "seqText" : "unSeqText"), false);
              break;
            case DOUBLE:
              valuePageWriter.write(
                  timestamp, isSeq ? timestamp + 0.01 : 100000.01 + timestamp, false);
              break;
            case BOOLEAN:
              valuePageWriter.write(timestamp, isSeq, false);
              break;
            case INT64:
              valuePageWriter.write(timestamp, isSeq ? timestamp : 100000L + timestamp, false);
              break;
            case INT32:
              valuePageWriter.write(timestamp, isSeq ? timestamp : 100000 + timestamp, false);
              break;
            case FLOAT:
              valuePageWriter.write(
                  timestamp, isSeq ? timestamp + 0.1 : 100000.1 + timestamp, false);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unknown data type " + valueChunkWriter.getDataType());
          }
        }
        // seal sub value page
        valueChunkWriter.sealCurrentPage();
      }
    }
    // seal time chunk
    alignedChunkWriter.getTimeChunkWriter().writeToFileWriter(tsFileIOWriter);
    // seal value pages
    for (Integer valueIndex : subSensorIndexList) {
      alignedChunkWriter.getValueChunkWriterByIndex(valueIndex).writeToFileWriter(tsFileIOWriter);
    }
  }

  private TsFileResource createEmptyFileAndResource(boolean isSeq) {
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + fileVersion
            + "-0-0.tsfile";
    String filePath;
    if (isSeq) {
      filePath = SEQ_DIRS.getPath() + File.separator + fileName;
    } else {
      filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
    }
    TsFileResource resource = new TsFileResource(new File(filePath));
    resource.updatePlanIndexes(fileVersion++);
    resource.setStatus(TsFileResourceStatus.CLOSED);
    return resource;
  }

  private void updateResource(
      TsFileResource resource, String device, long startTime, long endTime) {
    resource.updateStartTime(device, startTime);
    resource.updateEndTime(device, endTime);
  }

  private List<TSDataType> createDataType(int num) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      switch (i % 6) {
        case 0:
          dataTypes.add(TSDataType.BOOLEAN);
          break;
        case 1:
          dataTypes.add(TSDataType.INT32);
          break;
        case 2:
          dataTypes.add(TSDataType.INT64);
          break;
        case 3:
          dataTypes.add(TSDataType.FLOAT);
          break;
        case 4:
          dataTypes.add(TSDataType.DOUBLE);
          break;
        case 5:
          dataTypes.add(TSDataType.TEXT);
          break;
      }
    }
    return dataTypes;
  }

  protected void validateSeqFiles() {
    TsFileValidationTool.clearMap();
    List<File> files = new ArrayList<>();
    for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
      files.add(resource.getTsFile());
    }
    TsFileValidationTool.findUncorrectFiles(files);
    Assert.assertEquals(0, TsFileValidationTool.badFileNum);
  }

  private Map<PartialPath, List<TimeValuePair>> readSourceFiles(
      List<PartialPath> timeseriesPaths, List<TSDataType> dataTypes) throws IOException {
    Map<PartialPath, List<TimeValuePair>> sourceData = new LinkedHashMap<>();
    for (int i = 0; i < timeseriesPaths.size(); i++) {
      PartialPath path = timeseriesPaths.get(i);
      List<TimeValuePair> dataList = new ArrayList<>();
      sourceData.put(path, dataList);
      IDataBlockReader tsBlockReader =
          new SeriesDataBlockReader(
              path,
              dataTypes.get(i),
              FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                  EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
              seqResources,
              unseqResources,
              true);
      while (tsBlockReader.hasNextBatch()) {
        TsBlock block = tsBlockReader.nextBatch();
        IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
        while (iterator.hasNext()) {
          dataList.add(
              new TimeValuePair(
                  iterator.currentTime(), ((TsPrimitiveType[]) iterator.currentValue())[0]));
          // new Pair<>(iterator.currentTime(), ((TsPrimitiveType[]) iterator.currentValue())[0]));
          iterator.next();
        }
      }
    }
    return sourceData;
  }

  private void validateTargetDatas(
      Map<PartialPath, List<TimeValuePair>> sourceDatas, List<TSDataType> dataTypes)
      throws IOException {
    int timeseriesIndex = 0;
    for (Map.Entry<PartialPath, List<TimeValuePair>> entry : sourceDatas.entrySet()) {
      IDataBlockReader tsBlockReader =
          new SeriesDataBlockReader(
              entry.getKey(),
              dataTypes.get(timeseriesIndex++),
              FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                  EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
              tsFileManager.getTsFileList(true),
              Collections.emptyList(),
              true);
      List<TimeValuePair> timeseriesData = entry.getValue();
      while (tsBlockReader.hasNextBatch()) {
        TsBlock block = tsBlockReader.nextBatch();
        IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
        while (iterator.hasNext()) {
          TimeValuePair data = timeseriesData.remove(0);
          Assert.assertEquals(data.getTimestamp(), iterator.currentTime());
          Assert.assertEquals(data.getValue(), ((TsPrimitiveType[]) iterator.currentValue())[0]);
          iterator.next();
        }
      }
      if (timeseriesData.size() > 0) {
        // there are still data points left, which are not in the target file
        fail();
      }
    }
  }

  private void generateModsFile(
      List<PartialPath> seriesPaths, TsFileResource resource, long startValue, long endValue)
      throws IllegalPathException, IOException {
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (PartialPath path : seriesPaths) {
      deleteMap.put(path.getFullPath(), new Pair<>(startValue, endValue));
    }
    CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
  }
}
