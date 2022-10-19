package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.reader.SeriesDataBlockReader;
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
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

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
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    validateSeqFiles();
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

        List<Pair<Long, Long>> pages = new ArrayList<>();
        pages.add(new Pair<>(0L, 300L));
        pages.add(new Pair<>(500L, 600L));

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

        List<Pair<Long, Long>> pages = new ArrayList<>();
        pages.add(new Pair<>(200L, 2200L));

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

        List<Pair<Long, Long>> pages = new ArrayList<>();
        pages.add(new Pair<>(900L, 1400L));
        pages.add(new Pair<>(1550L, 1700L));
        pages.add(new Pair<>(1750L, 2000L));

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

        List<Pair<Long, Long>> pages = new ArrayList<>();
        pages.add(new Pair<>(550L, 800L));
        pages.add(new Pair<>(1200L, 1300L));
        pages.add(new Pair<>(1500L, 1600L));

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
    unseqResources.add(resource);

    Map<PartialPath, List<Pair<Long, Object>>> sourceDatas =
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

  public void writeNonAlignedChunk(
      ChunkWriterImpl chunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<Pair<Long, Long>> pages,
      boolean isSeq)
      throws IOException {
    PageWriter pageWriter = chunkWriter.getPageWriter();
    for (Pair<Long, Long> page : pages) {
      // write a page
      for (long timestamp = page.left; timestamp < page.right; timestamp++) {
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

  public void writeAlignedChunk(
      AlignedChunkWriterImpl alignedChunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<Pair<Long, Long>> pages,
      boolean isSeq,
      List<Integer> subSensorIndexList)
      throws IOException {
    TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
    for (Pair<Long, Long> page : pages) {
      // write time page
      for (long timestamp = page.left; timestamp < page.right; timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      // write value page
      for (Integer valueIndex : subSensorIndexList) {
        ValueChunkWriter valueChunkWriter =
            alignedChunkWriter.getValueChunkWriterByIndex(valueIndex);
        ValuePageWriter valuePageWriter = valueChunkWriter.getPageWriter();
        for (long timestamp = page.left; timestamp < page.right; timestamp++) {
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

  public TsFileResource createEmptyFileAndResource(boolean isSeq) {
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
    List<File> files = new ArrayList<>();
    for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
      files.add(resource.getTsFile());
    }
    TsFileValidationTool.findUncorrectFiles(files);
    Assert.assertEquals(0, TsFileValidationTool.badFileNum);
    TsFileValidationTool.clearMap();
  }

  private Map<PartialPath, List<Pair<Long, Object>>> readSourceFiles(
      List<PartialPath> timeseriesPaths, List<TSDataType> dataTypes) throws IOException {
    Map<PartialPath, List<Pair<Long, Object>>> sourceData = new LinkedHashMap<>();
    for (int i = 0; i < timeseriesPaths.size(); i++) {
      PartialPath path = timeseriesPaths.get(i);
      List<Pair<Long, Object>> dataList = new ArrayList<>();
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
          dataList.add(new Pair<>(iterator.currentTime(), iterator.currentValue()));
          iterator.next();
        }
      }
    }
    return sourceData;
  }

  private void validateTargetDatas(
      Map<PartialPath, List<Pair<Long, Object>>> sourceDatas, List<TSDataType> dataTypes)
      throws IOException {
    int timeseriesIndex = 0;
    for (Map.Entry<PartialPath, List<Pair<Long, Object>>> entry : sourceDatas.entrySet()) {
      IDataBlockReader tsBlockReader =
          new SeriesDataBlockReader(
              entry.getKey(),
              dataTypes.get(timeseriesIndex++),
              FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                  EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
              tsFileManager.getTsFileList(true),
              Collections.emptyList(),
              true);
      List<Pair<Long, Object>> timeseriesData = entry.getValue();
      while (tsBlockReader.hasNextBatch()) {
        TsBlock block = tsBlockReader.nextBatch();
        IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
        while (iterator.hasNext()) {
          Pair<Long, Object> data = timeseriesData.remove(0);
          Assert.assertEquals(data.left.longValue(), iterator.currentTime());
          TsPrimitiveType[] o = (TsPrimitiveType[]) iterator.currentValue();
          TsPrimitiveType[] right = (TsPrimitiveType[]) data.right;
          Assert.assertTrue(o[0].equals(right[0]));
          // Assert.assertTrue(data.right.equals(iterator.currentValue()));
          iterator.next();
        }
      }
    }
  }
}
