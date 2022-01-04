package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
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
    // setUnseqFileNum(2);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  //  @Test
  //  public void testInnerSpaceCompaction()
  //      throws MetadataException, IOException, StorageEngineException, WriteProcessException {
  //    // init(0);
  //    TsFileManager tsFileManager =
  //        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
  //    tsFileManager.addAll(seqResources, true);
  //    tsFileManager.addAll(unseqResources, false);
  //    MeasurementPath path =
  //        SchemaTestUtils.getMeasurementPath(
  //            deviceIds[0]
  //                + TsFileConstant.PATH_SEPARATOR
  //                + measurementSchemas[0].getMeasurementId());
  //    IBatchReader tsFilesReader =
  //        new SeriesRawDataBatchReader(
  //            path,
  //            measurementSchemas[0].getType(),
  //            EnvironmentUtils.TEST_QUERY_CONTEXT,
  //            tsFileManager.getTsFileList(true),
  //            new ArrayList<>(),
  //            null,
  //            null,
  //            true);
  //    int count = 0;
  //    while (tsFilesReader.hasNextBatch()) {
  //      BatchData batchData = tsFilesReader.nextBatch();
  //      for (int i = 0; i < batchData.length(); i++) {
  //        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
  //        count++;
  //      }
  //    }
  //    tsFilesReader.close();
  //    assertEquals(500, count);
  //    List<TsFileResource> targetResources =
  //        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources,
  // true);
  //    CompactionUtils.compact(seqResources, new ArrayList<>(), targetResources,
  // COMPACTION_TEST_SG);
  //    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);
  //    tsFileManager.removeAll(seqResources, true);
  //    tsFileManager.add(targetResources.get(0), true);
  //    tsFilesReader =
  //        new SeriesRawDataBatchReader(
  //            path,
  //            measurementSchemas[0].getType(),
  //            EnvironmentUtils.TEST_QUERY_CONTEXT,
  //            tsFileManager.getTsFileList(true),
  //            new ArrayList<>(),
  //            null,
  //            null,
  //            true);
  //    count = 0;
  //    while (tsFilesReader.hasNextBatch()) {
  //      BatchData batchData = tsFilesReader.nextBatch();
  //      for (int i = 0; i < batchData.length(); i++) {
  //        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
  //        count++;
  //      }
  //    }
  //    tsFilesReader.close();
  //    assertEquals(500, count);
  //  }
  //
  //  /**
  //   * cross space compaction with only one source seq file.
  //   *
  //   * @throws MetadataException
  //   * @throws IOException
  //   * @throws StorageEngineException
  //   */
  //  @Test
  //  public void testCrossSpaceCompaction()
  //      throws MetadataException, IOException, StorageEngineException, WriteProcessException {
  //    // init(1);
  //    MeasurementPath path =
  //        SchemaTestUtils.getMeasurementPath(
  //            deviceIds[0]
  //                + TsFileConstant.PATH_SEPARATOR
  //                + measurementSchemas[0].getMeasurementId());
  //    IBatchReader tsFilesReader =
  //        new SeriesRawDataBatchReader(
  //            path,
  //            measurementSchemas[0].getType(),
  //            EnvironmentUtils.TEST_QUERY_CONTEXT,
  //            seqResources,
  //            unseqResources,
  //            null,
  //            null,
  //            true);
  //    int count = 0;
  //    while (tsFilesReader.hasNextBatch()) {
  //      BatchData batchData = tsFilesReader.nextBatch();
  //      while (batchData.hasCurrent()) {
  //        if (batchData.currentTime() < 100) {
  //          assertEquals(batchData.currentTime() + 10000, (Double) batchData.currentValue(),
  // 0.001);
  //        } else {
  //          assertEquals(batchData.currentTime(), (Double) batchData.currentValue(), 0.001);
  //        }
  //        batchData.next();
  //        count++;
  //      }
  //    }
  //    tsFilesReader.close();
  //    assertEquals(500, count);
  //    List<TsFileResource> sourceSeqResources = new ArrayList<>();
  //    sourceSeqResources.add(seqResources.get(0));
  //    List<TsFileResource> targetResources =
  //
  // CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(sourceSeqResources);
  //    CompactionUtils.compact(
  //        sourceSeqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
  //    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);
  //    seqResources.remove(0);
  //    tsFileManager.add(targetResources.get(0), true);
  //    tsFileManager.addAll(seqResources, true);
  //    tsFilesReader =
  //        new SeriesRawDataBatchReader(
  //            path,
  //            measurementSchemas[0].getType(),
  //            EnvironmentUtils.TEST_QUERY_CONTEXT,
  //            tsFileManager.getTsFileList(true),
  //            tsFileManager.getTsFileList(false),
  //            null,
  //            null,
  //            true);
  //    count = 0;
  //    while (tsFilesReader.hasNextBatch()) {
  //      BatchData batchData = tsFilesReader.nextBatch();
  //      for (int i = 0; i < batchData.length(); i++) {
  //        if (batchData.getTimeByIndex(i) < 100) {
  //          assertEquals(batchData.getTimeByIndex(i) + 10000, batchData.getDoubleByIndex(i),
  // 0.001);
  //        } else {
  //          assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
  //        }
  //        count++;
  //      }
  //    }
  //    tsFilesReader.close();
  //    assertEquals(500, count);
  //  }
  //
  //  /**
  //   * cross space compaction with two source seq file.
  //   *
  //   * @throws MetadataException
  //   * @throws IOException
  //   * @throws StorageEngineException
  //   */
  //  @Test
  //  public void testCrossSpaceCompaction2()
  //      throws MetadataException, IOException, StorageEngineException, WriteProcessException {
  //    // init(2);
  //    MeasurementPath path =
  //        SchemaTestUtils.getMeasurementPath(
  //            deviceIds[0]
  //                + TsFileConstant.PATH_SEPARATOR
  //                + measurementSchemas[0].getMeasurementId());
  //    IBatchReader tsFilesReader =
  //        new SeriesRawDataBatchReader(
  //            path,
  //            measurementSchemas[0].getType(),
  //            EnvironmentUtils.TEST_QUERY_CONTEXT,
  //            seqResources,
  //            unseqResources,
  //            null,
  //            null,
  //            true);
  //    int count = 0;
  //    while (tsFilesReader.hasNextBatch()) {
  //      BatchData batchData = tsFilesReader.nextBatch();
  //      while (batchData.hasCurrent()) {
  //        if (batchData.currentTime() < 50) {
  //          assertEquals(batchData.currentTime() + 10000, (Double) batchData.currentValue(),
  // 0.001);
  //        } else if (199 < batchData.currentTime() && batchData.currentTime() < 300) {
  //          assertEquals(batchData.currentTime() + 10000, (Double) batchData.currentValue(),
  // 0.001);
  //        } else {
  //          assertEquals(batchData.currentTime(), (Double) batchData.currentValue(), 0.001);
  //        }
  //        batchData.next();
  //        count++;
  //      }
  //    }
  //    tsFilesReader.close();
  //    assertEquals(500, count);
  //    List<TsFileResource> sourceSeqResources = new ArrayList<>();
  //    sourceSeqResources.add(seqResources.get(0));
  //    sourceSeqResources.add(seqResources.get(2));
  //    List<TsFileResource> targetResources =
  //
  // CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(sourceSeqResources);
  //    CompactionUtils.compact(
  //        sourceSeqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
  //    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);
  //    tsFileManager.add(targetResources.get(0), true);
  //    tsFileManager.add(seqResources.get(1), true);
  //    tsFileManager.add(targetResources.get(1), true);
  //    tsFileManager.add(seqResources.get(3), true);
  //    tsFileManager.add(seqResources.get(4), true);
  //    tsFilesReader =
  //        new SeriesRawDataBatchReader(
  //            path,
  //            measurementSchemas[0].getType(),
  //            EnvironmentUtils.TEST_QUERY_CONTEXT,
  //            tsFileManager.getTsFileList(true),
  //            tsFileManager.getTsFileList(false),
  //            null,
  //            null,
  //            true);
  //    count = 0;
  //    while (tsFilesReader.hasNextBatch()) {
  //      BatchData batchData = tsFilesReader.nextBatch();
  //      while (batchData.hasCurrent()) {
  //        if (batchData.currentTime() < 50) {
  //          assertEquals(batchData.currentTime() + 10000, (Double) batchData.currentValue(),
  // 0.001);
  //        } else if (199 < batchData.currentTime() && batchData.currentTime() < 300) {
  //          assertEquals(batchData.currentTime() + 10000, (Double) batchData.currentValue(),
  // 0.001);
  //        } else {
  //          assertEquals(batchData.currentTime(), (Double) batchData.currentValue(), 0.001);
  //        }
  //        batchData.next();
  //        count++;
  //      }
  //    }
  //    tsFilesReader.close();
  //    assertEquals(500, count);
  //  }
  //

  @Test
  public void testAlignedInnerSpaceCompaction()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 5, true);
    List<TsFileResource> alignedSeqResources = new ArrayList<>();
    int alignedSeqFileNum = 5;
    for (int i = 0; i < alignedSeqFileNum; i++) {
      String filePath = TsFileGeneratorUtils.getTsFilePath(SEQ_DIRS.getPath(), i + seqFileNum + 1);
      TsFileResource alignedResource =
          new TsFileResource(
              TsFileGeneratorUtils.generateAlignedTsFile(
                  filePath, 2, 5, 100, 100 * i, 100 * i, 0, 0));
      alignedResource.updatePlanIndexes((long) i);
      alignedResource.setClosed(true);
      alignedSeqResources.add(alignedResource);
    }

    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    AlignedPath path =
        new AlignedPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1", Collections.singletonList("s1"), schemas);
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            alignedSeqResources,
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(
            alignedSeqResources, true);
    CompactionUtils.compact(
        alignedSeqResources, new ArrayList<>(), targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            Collections.singletonList(targetResources.get(0)),
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
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

    //    List<TsFileResource> alignedSeqResources = new ArrayList<>();
    //    int alignedSeqFileNum = 5;
    //    for (int i = 0; i < alignedSeqFileNum; i++) {
    //
    //      String filePath = TsFileGeneratorUtils.getTsFilePath(tempSGDir.getPath(), i +
    // seqFileNum);
    //      TsFileResource alignedResource =
    //          new TsFileResource(
    //              TsFileGeneratorUtils.generateAlignedTsFile(
    //                  filePath, 2, 5, 100, 100 * i, 100 * i, 0, 0));
    //      alignedResource.updatePlanIndexes((long) i);
    //      alignedResource.updateStartTime(
    //          TsFileGeneratorUtils.testStorageGroup + PATH_SEPARATOR + "d0", 100 * i);
    //      alignedResource.updateEndTime(
    //          TsFileGeneratorUtils.testStorageGroup + PATH_SEPARATOR + "d0", 100 * i + 100 -
    // 1);
    //      alignedResource.updateStartTime(
    //          TsFileGeneratorUtils.testStorageGroup + PATH_SEPARATOR + "d1", 100 * i);
    //      alignedResource.updateEndTime(
    //          TsFileGeneratorUtils.testStorageGroup + PATH_SEPARATOR + "d1", 100 * i + 100 -
    // 1);
    //      alignedResource.setClosed(true);
    //      alignedSeqResources.add(alignedResource);
    //    }
    //    List<TsFileResource> alignedUnseqResources = new ArrayList<>();
    //    int alignedUnseqFileNum = 2;
    //    for (int i = 0; i < alignedUnseqFileNum; i++) {
    //      String filePath =
    //          TsFileGeneratorUtils.getTsFilePath(
    //              tempSGDir.getPath().replace("sequence", "unsequence"),
    //              i + seqFileNum + alignedSeqFileNum);
    //      TsFileResource alignedResource =
    //          new TsFileResource(
    //              TsFileGeneratorUtils.generateAlignedTsFile(
    //                  filePath, 2, 5, 50, 200 * i, 10000 * (i + 1), 0, 0));
    //      alignedResource.updatePlanIndexes((long) i);
    //      alignedResource.setClosed(true);
    //      alignedUnseqResources.add(alignedResource);
    //    }
    //
    //    List<TsFileResource> resourcesToBeCompacted = new ArrayList<>();
    //    resourcesToBeCompacted.add(alignedSeqResources.get(0));
    //    resourcesToBeCompacted.add(alignedSeqResources.get(2));
    //    List<TsFileResource> targetResources =
    //        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(
    //            resourcesToBeCompacted);
    //    CompactionUtils.compact(
    //        resourcesToBeCompacted, alignedUnseqResources, targetResources,
    // COMPACTION_TEST_SG);
    //    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);
  }
}
