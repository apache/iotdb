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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
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

  /* Total 5 files, each file has the same 6 nonAligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
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

  /*
  Total 6 files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s4, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  */
  @Test
  public void testSeqInnerSpaceCompactionWithDifferentTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(5, 5, false);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    createFiles(2, 5, 5, 50, 600, 800, 50, 50, false, true);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
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
            if (batchData.currentTime() >= 600) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < 3) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
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
            if (batchData.currentTime() >= 600) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < 3 && j < 5) {
          assertEquals(200, count);
        } else if (i < 5 && j < 5) {
          assertEquals(100, count);
        }
      }
    }
  }

  /* Total 5 files, each file has the same 6 nonAligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testUnSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, false, false);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s1",
                new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
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
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s1",
                new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
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
      }
    }
  }

  /*
  Total 10 files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 150 ~ 199 and 250 ~ 299, value range is 150 ~ 199 and 250 ~ 299.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s4, time range is 100 ~ 149 and 250 ~ 299, value range is 100 ~ 149 and 250 ~ 299.
  Seventh and Eighth file: d0 ~ d6 and s0 ~ s6, time range is 200 ~ 269 and 370 ~ 439, value range is 300 ~ 369 and 470 ~ 539.
  Ninth and Tenth file: d0 ~ d8 and s0 ~ s8, time range is 100 ~ 169 and 270 ~ 339, value range is 300 ~ 369 and 470 ~ 539.
  */
  @Test
  public void testUnSeqInnerSpaceCompactionWithDifferentTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(9, 9, false);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, false);
    createFiles(2, 3, 5, 50, 150, 150, 50, 50, false, false);
    createFiles(2, 5, 5, 50, 100, 100, 100, 100, false, false);
    createFiles(2, 7, 7, 70, 200, 300, 100, 100, false, false);
    createFiles(2, 9, 9, 70, 100, 300, 100, 100, false, false);

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
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
            if ((100 <= batchData.currentTime() && batchData.currentTime() < 170)
                || (270 <= batchData.currentTime() && batchData.currentTime() < 340)) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else if ((200 <= batchData.currentTime() && batchData.currentTime() < 270)
                || (370 <= batchData.currentTime() && batchData.currentTime() < 440)) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(410, count);
        } else if (i < 3 && j < 5) {
          assertEquals(310, count);
        } else if (i < 5 && j < 5) {
          assertEquals(280, count);
        } else if (i < 7 && j < 7) {
          assertEquals(280, count);
        } else {
          assertEquals(140, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new UnaryMeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if ((100 <= batchData.currentTime() && batchData.currentTime() < 170)
                || (270 <= batchData.currentTime() && batchData.currentTime() < 340)) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else if ((200 <= batchData.currentTime() && batchData.currentTime() < 270)
                || (370 <= batchData.currentTime() && batchData.currentTime() < 440)) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(410, count);
        } else if (i < 3 && j < 5) {
          assertEquals(310, count);
        } else if (i < 5 && j < 5) {
          assertEquals(280, count);
        } else if (i < 7 && j < 7) {
          assertEquals(280, count);
        } else {
          assertEquals(140, count);
        }
      }
    }
  }

  /* Total 5 files, each file has the same 6 aligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithSameTimeseries()
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

  /*
  Total 6 files, each file has different aligned timeseries, which may cause empty value page.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  */
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithDifferentTimeseriesAndEmptyPage()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(50);
    registerTimeseriesInMManger(5, 5, true);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, true, true);
    createFiles(2, 5, 7, 50, 600, 800, 50, 50, true, true);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
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
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
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
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
  }

  /*
  Total 6 files, each file has different aligned timeseries, which may cause empty value chunk.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  */
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithDifferentTimeseriesAndEmptyChunk()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(5, 5, true);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, true, true);
    createFiles(2, 5, 7, 50, 600, 800, 50, 50, true, true);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
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
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
                false);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
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
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
  }

  /* Total 5 files, each file has the same 6 aligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testAlignedUnSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, true, false);

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
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
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
                new ArrayList<>(),
                targetResources,
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

  private void testReader(SeriesRawDataBatchReader reader, int deviceNum, int measurementNum) {}
}
