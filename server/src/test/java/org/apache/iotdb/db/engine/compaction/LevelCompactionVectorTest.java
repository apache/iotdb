package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
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
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.VectorChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

public class LevelCompactionVectorTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";

  protected int seqFileNum = 6;
  int unseqFileNum = 0;
  protected int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  List<IMeasurementSchema> measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private int prevMergeChunkThreshold;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    IoTDB.metaManager.init();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(-1);
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkThreshold);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new ArrayList<>(measurementNum);
    for (int i = 0; i < measurementNum; i++) {
      if (i % 2 == 0) {
        String[] measurements = new String[2];
        measurements[0] = "sensor0";
        measurements[1] = "sensor1";

        List<Integer> dataTypesList = new ArrayList<>();
        TSDataType[] dataTypes = new TSDataType[2];
        dataTypesList.add(TSDataType.BOOLEAN.ordinal());
        dataTypesList.add(TSDataType.INT64.ordinal());
        dataTypes[0] = TSDataType.BOOLEAN;
        dataTypes[1] = TSDataType.INT64;

        TSEncoding[] encodings = new TSEncoding[2];
        encodings[0] = TSEncoding.PLAIN;
        encodings[1] = TSEncoding.GORILLA;

        measurementSchemas.add(
            new VectorMeasurementSchema(
                IoTDBConstant.ALIGN_TIMESERIES_PREFIX + 0, measurements, dataTypes, encodings));
      } else {
        measurementSchemas.add(
            new MeasurementSchema(
                "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED));
      }
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = COMPACTION_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String device : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        if (measurementSchema instanceof MeasurementSchema) {
          IoTDB.metaManager.createTimeseries(
              devicePath.concatNode(measurementSchema.getMeasurementId()),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getCompressor(),
              Collections.emptyMap());
        } else {
          VectorMeasurementSchema vectorMeasurementSchema =
              (VectorMeasurementSchema) measurementSchema;
          List<String> valueMeasurementIdList = vectorMeasurementSchema.getValueMeasurementIdList();
          List<TSDataType> dataTypeList = vectorMeasurementSchema.getValueTSDataTypeList();
          List<TSEncoding> tsEncodingList = vectorMeasurementSchema.getValueTSEncodingList();
          for (int i = 0; i < valueMeasurementIdList.size(); i++) {
            IoTDB.metaManager.createTimeseries(
                devicePath.concatNode(valueMeasurementIdList.get(i)),
                dataTypeList.get(i),
                tsEncodingList.get(i),
                measurementSchema.getCompressor(),
                Collections.emptyMap());
          }
        }
      }
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException {
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  (10000 + i)
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + (10000 + i)
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                unseqFileNum
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + unseqFileNum
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) (seqFileNum + unseqFileNum));
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
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
      throws IOException {
    RestorableTsFileIOWriter fileWriter = new RestorableTsFileIOWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      fileWriter.startChunkGroup(deviceId);
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        if (measurementSchema instanceof MeasurementSchema) {
          IChunkWriter chunkWriter = new ChunkWriterImpl(measurementSchema);
          for (long i = timeOffset; i < timeOffset + ptNum; i++) {
            chunkWriter.write(i, (double) i + valueOffset, false);
            tsFileResource.updateStartTime(deviceId, i);
            tsFileResource.updateEndTime(deviceId, i);
          }
        } else {
          IChunkWriter chunkWriter = new VectorChunkWriterImpl(measurementSchema);
          for (long i = timeOffset; i < timeOffset + ptNum; i++) {
            chunkWriter.write(i, true, false);
            chunkWriter.write(i, i + valueOffset, false);
            chunkWriter.write(i);
            tsFileResource.updateStartTime(deviceId, i);
            tsFileResource.updateEndTime(deviceId, i);
          }
        }
      }
    }
    fileWriter.close();
  }
}
