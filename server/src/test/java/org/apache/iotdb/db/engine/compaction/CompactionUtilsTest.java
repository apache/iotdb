package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTest;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaTestUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CompactionUtilsTest extends AbstractInnerSpaceCompactionTest {
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    setUnseqFileNum(1);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testInnerSpaceCompaction()
      throws MetadataException, IOException, StorageEngineException {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, new ArrayList<>(), targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);
    tsFileManager.removeAll(seqResources, true);
    tsFileManager.add(targetResources.get(0), true);
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }

  @Test
  public void testCrossSpaceCompaction()
      throws MetadataException, IOException, StorageEngineException {
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
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
        if (batchData.currentTime() < 100) {
          assertEquals(batchData.currentTime() + 10000, (Double) batchData.currentValue(), 0.001);
        } else {
          assertEquals(batchData.currentTime(), (Double) batchData.currentValue(), 0.001);
        }
        batchData.next();
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
    List<TsFileResource> sourceSeqResources = new ArrayList<>();
    sourceSeqResources.add(seqResources.get(0));
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(sourceSeqResources);
    CompactionUtils.compact(
        sourceSeqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);
    seqResources.remove(0);
    tsFileManager.add(targetResources.get(0), true);
    tsFileManager.addAll(seqResources, true);
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
            tsFileManager.getTsFileList(false),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) < 100) {
          assertEquals(batchData.getTimeByIndex(i) + 10000, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        }
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }
}
