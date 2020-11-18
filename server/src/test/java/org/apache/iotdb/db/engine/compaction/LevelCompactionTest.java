package org.apache.iotdb.db.engine.compaction;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LevelCompactionTest {

  private boolean compactionMergeWorking = false;
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  int seqFileNum = 10;
  int unseqFileNum = 0;
  int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();
  private File tempSGDir;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    EnvironmentUtils.envSetUp();
    CompactionMergeTaskPoolManager.getInstance().start();
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    EnvironmentUtils.cleanEnv();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    CompactionMergeTaskPoolManager.getInstance().stop();
    FileUtils.deleteDirectory(tempSGDir);
  }

  private void prepareSeries() throws MetadataException, MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] = new MeasurementSchema("sensor" + i, TSDataType.DOUBLE,
          encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = COMPACTION_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        IoTDB.metaManager.createTimeseries(
            devicePath.concatNode(measurementSchema.getMeasurementId()), measurementSchema
                .getType(), measurementSchema.getEncodingType(), measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum)
      throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
          i + "seq" + IoTDBConstant.FILE_NAME_SEPARATOR + i + IoTDBConstant.FILE_NAME_SEPARATOR
              + i + IoTDBConstant.FILE_NAME_SEPARATOR + 0
              + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setHistoricalVersions(Collections.singleton((long) i));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
          i + "unseq" + IoTDBConstant.FILE_NAME_SEPARATOR
              + i + IoTDBConstant.FILE_NAME_SEPARATOR
              + i + IoTDBConstant.FILE_NAME_SEPARATOR + 0
              + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setHistoricalVersions(Collections.singleton((long) (i + seqFileNum)));
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file = new File(TestConstant.BASE_OUTPUT_PATH
        .concat(unseqFileNum + "unseq" + IoTDBConstant.FILE_NAME_SEPARATOR + unseqFileNum
            + IoTDBConstant.FILE_NAME_SEPARATOR + unseqFileNum
            + IoTDBConstant.FILE_NAME_SEPARATOR + 0 + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setHistoricalVersions(Collections.singleton((long) (seqFileNum + unseqFileNum)));
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    FileReaderManager.getInstance().stop();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum,
      long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(DataPoint.getDataPoint(measurementSchemas[k].getType(),
              measurementSchemas[k].getMeasurementId(), String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }

  @Test
  public void testLevelCompaction() throws Exception {
    LevelCompactionTsFileManagement compactionTsFileManagement = new LevelCompactionTsFileManagement(
        COMPACTION_TEST_SG, tempSGDir.getPath());
    compactionTsFileManagement.forkCurrentFileList(0);
    this.compactionMergeWorking = true;
    CompactionMergeTaskPoolManager.getInstance()
        .submitTask(
            compactionTsFileManagement.new CompactionMergeTask(this::closeCompactionMergeCallBack,
                0));
    while (compactionMergeWorking) {
      // wait
    }

    QueryContext context = new QueryContext();
    PartialPath path = new PartialPath(
        deviceIds[0] + TsFileConstant.PATH_SEPARATOR + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(0));
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        resources, new ArrayList<>(), null, null, true);
    int cnt = 0;
    try {
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        for (int i = 0; i < batchData.length(); i++) {
          cnt++;
        }
      }
      assertEquals(100, cnt);
    } finally {
      tsFilesReader.close();
    }
  }

  private void closeCompactionMergeCallBack() {
    this.compactionMergeWorking = false;
  }
}
