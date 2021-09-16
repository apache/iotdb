package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class ResourceManagerTest {

  static final String RESOURCE_MANAGER_TEST_SG = "root.resourceManagerTest";
  private int seqFileNum = 10;
  private int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();;
  private double timeIndexMemoryProportion;
  private long allocateMemoryForRead;
  private TimeIndexLevel timeIndexLevel;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    IoTDB.metaManager.init();
    timeIndexMemoryProportion = CONFIG.getTimeIndexMemoryProportion();
    allocateMemoryForRead = CONFIG.getAllocateMemoryForRead();
    timeIndexLevel = CONFIG.getTimeIndexLevel();
    prepareSeries();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    CONFIG.setTimeIndexMemoryProportion(timeIndexMemoryProportion);
    CONFIG.setTimeIndexLevel(String.valueOf(timeIndexLevel));
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    TsFileResourceManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = RESOURCE_MANAGER_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(RESOURCE_MANAGER_TEST_SG));
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        IoTDB.metaManager.createTimeseries(
            devicePath.concatNode(measurementSchema.getMeasurementId()),
            measurementSchema.getType(),
            measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unseqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    FileReaderManager.getInstance().stop();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
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
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i + valueOffset)));
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
  public void testReduceMemory() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    long previousRamSize = tsFileResource.calculateRamSize();
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long reducedMemory = tsFileResource.degradeTimeIndex();
    assertEquals(previousRamSize - tsFileResource.calculateRamSize(), reducedMemory);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testDegradeToFileTimeIndex() throws IOException, WriteProcessException {
    double smallMemoryProportion = Math.pow(10, -6);
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    tsFileResourceManager.setTimeIndexMemoryThreshold(smallMemoryProportion);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testNotDegradeToFileTimeIndex() throws IOException, WriteProcessException {
    double largeMemoryProportion = Math.pow(10, -5);
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long previousRamSize = tsFileResource.calculateRamSize();
    tsFileResourceManager.setTimeIndexMemoryThreshold(largeMemoryProportion);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(0, previousRamSize - tsFileResource.calculateRamSize());
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testTwoResourceToDegrade() throws IOException, WriteProcessException {
    double smallMemoryProportion = Math.pow(10, -5);
    File file1 =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource1 = new TsFileResource(file1);
    tsFileResource1.setClosed(true);
    tsFileResource1.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource1, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    tsFileResourceManager.setTimeIndexMemoryThreshold(smallMemoryProportion);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource1);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    File file2 =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource2 = new TsFileResource(file2);
    tsFileResource2.setClosed(true);
    tsFileResource2.updatePlanIndexes((long) 1);
    prepareFile(tsFileResource2, ptNum, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource2);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
  }

  @Test
  public void testMultiDeviceTimeIndexDegrade() throws IOException, WriteProcessException {
    double timeIndexMemoryProportion = 3 * Math.pow(10, -5);
    tsFileResourceManager.setTimeIndexMemoryThreshold(timeIndexMemoryProportion);
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes((long) i);
      assertEquals(
          TimeIndexLevel.DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    }
    for (int i = 0; i < seqFileNum; i++) {
      if (i < 7) {
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      } else {
        assertEquals(
            TimeIndexLevel.DEVICE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      }
    }
  }

  @Test(expected = RuntimeException.class)
  public void testAllFileTimeIndexDegrade() throws IOException, WriteProcessException {
    double timeIndexMemoryProportion = Math.pow(10, -6);
    long reducedMemory = 0;
    CONFIG.setTimeIndexLevel(String.valueOf(TimeIndexLevel.FILE_TIME_INDEX));
    tsFileResourceManager.setTimeIndexMemoryThreshold(timeIndexMemoryProportion);
    try {
      for (int i = 0; i < seqFileNum; i++) {
        File file =
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    i
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + i
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
        TsFileResource tsFileResource = new TsFileResource(file);
        tsFileResource.setClosed(true);
        tsFileResource.updatePlanIndexes((long) i);
        seqResources.add(tsFileResource);
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
        long previousRamSize = tsFileResource.calculateRamSize();
        prepareFile(tsFileResource, i * ptNum, ptNum, 0);
        tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
        reducedMemory = previousRamSize - tsFileResource.calculateRamSize();
      }
    } catch (RuntimeException e) {
      assertEquals(0, reducedMemory);
      assertEquals(7, seqResources.size());
      throw e;
    }
  }

}
