package org.apache.iotdb.db.engine.migration.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

public abstract class MigrationTest {
  static final FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  static final String testSgName = "root.migrationTest";
  static final String testSgSysDir =
      TestConstant.OUTPUT_DATA_DIR.concat(testSgName).concat(File.separator);
  static final String testSgTier1Dir =
      TestConstant.OUTPUT_DATA_DIR
          .concat("tier1")
          .concat(File.separator)
          .concat(testSgName)
          .concat(File.separator);
  static final String testSgTier2Dir =
      TestConstant.OUTPUT_DATA_DIR
          .concat("tier2")
          .concat(File.separator)
          .concat(testSgName)
          .concat(File.separator);

  final int srcFileNum = 6;
  final int measurementNum = 10;
  final int deviceNum = 10;
  final long ptNum = 100;
  final long flushInterval = 20;
  final TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> srcResources = new ArrayList<>();
  List<File> srcFiles = new ArrayList<>();
  File targetDir = fsFactory.getFile(testSgTier2Dir);

  @Before
  public void setUp() throws Exception {
    fsFactory.getFile(testSgTier1Dir).mkdirs();
    fsFactory.getFile(testSgTier2Dir).mkdirs();
    IoTDB.metaManager.init();
    prepareSeries();
    prepareFiles(srcFileNum);
  }

  @After
  public void tearDown() throws Exception {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  private void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = testSgName + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(testSgName));
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

  private void prepareFiles(int fileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < fileNum; ++i) {
      File file =
          fsFactory.getFile(
              testSgTier1Dir,
              i
                  + IoTDBConstant.FILE_NAME_SEPARATOR
                  + 0
                  + IoTDBConstant.FILE_NAME_SEPARATOR
                  + 0
                  + ".tsfile");
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i);
      srcFiles.add(file);
      srcResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum);
    }
  }

  private void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum)
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
                  String.valueOf(i)));
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
    tsFileResource.serialize();
  }
}
