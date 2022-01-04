package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class CompactionUtilsTest extends AbstractCompactionTest {

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testAlignedSeqInnerSpaceCompaction()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, true, true);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(
                batchData.currentTime(),
                ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(
                batchData.currentTime(),
                ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
  }

  @Test
  public void testSeqInnerSpaceCompaction()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    // IoTDB.metaManager.setStorageGroup(newPartialPath(TsFileGeneratorUtils.testStorageGroup));
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, false, true);

    PartialPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1",
            "s1",
            new UnaryMeasurementSchema("s1", TSDataType.INT64));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResources,
            unseqResources,
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        assertEquals(batchData.currentTime(), batchData.currentValue());
        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            targetResources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        assertEquals(batchData.currentTime(), batchData.currentValue());
        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }

  @Test
  public void testCrossSpaceCompaction()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    // IoTDB.metaManager.setStorageGroup(newPartialPath(TsFileGeneratorUtils.testStorageGroup));
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    PartialPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1",
            "s1",
            new UnaryMeasurementSchema("s1", TSDataType.INT64));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResources,
            unseqResources,
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
        } else {
          assertEquals(batchData.currentTime(), batchData.currentValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            targetResources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
        } else {
          assertEquals(batchData.currentTime(), batchData.currentValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }

  @Test
  public void testAlignedCrossSpaceCompaction()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    // IoTDB.metaManager.setStorageGroup(newPartialPath(TsFileGeneratorUtils.testStorageGroup));
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, true, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, true, false);

    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    AlignedPath path =
        new AlignedPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000",
            Collections.singletonList("s1"),
            schemas);
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.VECTOR,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResources,
            unseqResources,
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(
              batchData.currentTime() + 10000,
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        } else {
          assertEquals(
              batchData.currentTime(),
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            targetResources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(
              batchData.currentTime() + 10000,
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        } else {
          assertEquals(
              batchData.currentTime(),
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }
}
