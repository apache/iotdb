package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class FastCompactionPerformerTest extends AbstractCompactionTest {
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
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

  /**
   * Total 5 seq files and 5 unseq files, each file has the same nonAligned timeseries
   *
   * <p>Seq files has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99, 100 ~ 199, 200 ~ 299, 300 ~ 399 and
   * 400 ~ 499, value range is 0 ~ 99, 100 ~ 199, 200 ~ 299, 300 ~ 399 and 400 ~ 499.
   *
   * <p>UnSeq files has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 49, 100 ~ 149, 200 ~ 249, 300 ~ 349
   * and 400 ~ 449, value range is 10000 ~ 10049, 10100 ~ 10149, 10200 ~ 10249, 10300 ~ 10349 and
   * 10400 ~ 10449.
   */
  @Test
  public void testCrossSpaceCompactionWithSameTimeseries() throws Exception {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    PartialPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1",
            "s1",
            new MeasurementSchema("s1", TSDataType.INT64));
    IDataBlockReader tsBlockReader =
        new SeriesDataBlockReader(
            path,
            TSDataType.INT64,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            seqResources,
            unseqResources,
            true);
    int count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
      while (iterator.hasNext()) {
        if (iterator.currentTime() % 100 < 50) {
          assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
        } else {
          assertEquals(iterator.currentTime(), iterator.currentValue());
        }

        count++;
        iterator.next();
      }
    }
    tsBlockReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    tsBlockReader =
        new SeriesDataBlockReader(
            path,
            TSDataType.INT64,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            targetResources,
            new ArrayList<>(),
            true);
    count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
      while (iterator.hasNext()) {
        if (iterator.currentTime() % 100 < 50) {
          assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
        } else {
          assertEquals(iterator.currentTime(), iterator.currentValue());
        }

        count++;
        iterator.next();
      }
    }
    tsBlockReader.close();
    assertEquals(500, count);
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   */
  @Test
  public void testCrossSpaceCompactionWithDifferentTimeseries() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    List<String> deviceIdList = new ArrayList<>();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   */
  @Test
  public void testCrossSpaceCompactionWithDifferentTimeseries2() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(4, 1, 1, 100, 200, 200, 0, 0, false, true);
    createFiles(2, 1, 3, 100, 250, 10250, 0, 0, false, false);
    createFiles(1, 1, 3, 190, 210, 30210, 0, 0, false, false);
    createFiles(1, 1, 3, 70, 430, 40430, 0, 0, false, false);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    List<String> deviceIdList = new ArrayList<>();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0.s0, d0.s1, d2.s4 and d3.s4 is deleted in each file.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInTimeseries() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0");
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s1");
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s4");
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3" + PATH_SEPARATOR + "s4");
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if ((i == 0 && j == 0) || (i == 0 && j == 1) || (i == 2 && j == 4) || (i == 3 && j == 4)) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    List<String> deviceIdList = new ArrayList<>();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if ((i == 0 && j == 0) || (i == 0 && j == 1) || (i == 2 && j == 4) || (i == 3 && j == 4)) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0 and d2 is deleted in each file.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInDevice() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 2) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    List<String> deviceIdList = new ArrayList<>();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 2; i < 4; i++) {
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 2) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0, d1 and d2 is deleted in each file. Data in the first and second target file
   * is all deleted.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInOneTargetFile() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    targetResources.removeIf(resource -> resource == null);
    Assert.assertEquals(2, targetResources.size());
    List<String> deviceIdList = new ArrayList<>();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0, d1 and d2 is deleted in each seq file.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInDeviceInSeqFiles() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 1) {
          if (j < 4) {
            assertEquals(630, count);
          } else {
            assertEquals(200, count);
          }
        } else if (i < 3) {
          if (j < 4) {
            assertEquals(600, count);
          } else {
            assertEquals(0, count);
          }
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

    Assert.assertEquals(4, targetResources.size());
    List<String> deviceIdList = new ArrayList<>();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 2; i < 3; i++) {
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.clear();
    deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
    for (int i = 3; i < 4; i++) {
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      Assert.assertFalse(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      Assert.assertTrue(
          targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 1) {
          if (j < 4) {
            assertEquals(630, count);
          } else {
            assertEquals(200, count);
          }
        } else if (i < 3) {
          if (j < 4) {
            assertEquals(600, count);
          } else {
            assertEquals(0, count);
          }
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  @Test
  public void testCrossSpaceCompactionWithNewDeviceInUnseqFile() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    try {
      registerTimeseriesInMManger(6, 6, false);
      createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
      createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
      createFiles(3, 6, 6, 200, 20, 10020, 30, 30, false, false);
      createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

      List<TsFileResource> targetResources =
          CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
      ICompactionPerformer performer =
          new FastCompactionPerformer(seqResources, unseqResources, targetResources);
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

      Assert.assertEquals(4, targetResources.size());
      List<String> deviceIdList = new ArrayList<>();
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
      for (int i = 0; i < 2; i++) {
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
        Assert.assertFalse(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
        Assert.assertFalse(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
        check(targetResources.get(i), deviceIdList);
      }
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
      for (int i = 2; i < 3; i++) {
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
        check(targetResources.get(i), deviceIdList);
      }
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d4");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d5");
      for (int i = 3; i < 4; i++) {
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d4"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d5"));
        check(targetResources.get(i), deviceIdList);
      }
    } catch (MetadataException
        | IOException
        | WriteProcessException
        | StorageEngineException
        | InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testCrossSpaceCompactionWithDeviceMaxTimeLaterInUnseqFile() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    try {
      registerTimeseriesInMManger(6, 6, false);
      createFiles(2, 2, 3, 200, 0, 0, 0, 0, false, true);
      createFiles(3, 4, 4, 300, 20, 10020, 0, 0, false, false);

      List<TsFileResource> targetResources =
          CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
      ICompactionPerformer performer =
          new FastCompactionPerformer(seqResources, unseqResources, targetResources);
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);

      Assert.assertEquals(2, targetResources.size());
      List<String> deviceIdList = new ArrayList<>();
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1");
      for (int i = 0; i < 1; i++) {
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
        Assert.assertFalse(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
        Assert.assertFalse(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
        check(targetResources.get(i), deviceIdList);
      }
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2");
      deviceIdList.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3");
      for (int i = 1; i < 2; i++) {
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
        Assert.assertTrue(
            targetResources.get(i).isDeviceIdExist(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
        check(targetResources.get(i), deviceIdList);
      }

      for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 4; j++) {
          PartialPath path =
              new MeasurementPath(
                  COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                  "s" + j,
                  new MeasurementSchema("s" + j, TSDataType.INT64));
          IDataBlockReader tsBlockReader =
              new SeriesDataBlockReader(
                  path,
                  TSDataType.INT64,
                  FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                      EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                  targetResources,
                  new ArrayList<>(),
                  true);
          int count = 0;
          while (tsBlockReader.hasNextBatch()) {
            TsBlock block = tsBlockReader.nextBatch();
            IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
            while (iterator.hasNext()) {
              if (iterator.currentTime() < 20) {
                assertEquals(iterator.currentTime(), iterator.currentValue());
              } else {
                assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
              }
              count++;
              iterator.next();
            }
          }
          tsBlockReader.close();
          if (i < 2 && j < 3) {
            assertEquals(920, count);
          } else {
            assertEquals(900, count);
          }
        }
      }
    } catch (MetadataException
        | IOException
        | WriteProcessException
        | StorageEngineException
        | InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private void generateModsFile(
      List<String> seriesPaths, List<TsFileResource> resources, long startValue, long endValue)
      throws IllegalPathException, IOException {
    for (TsFileResource resource : resources) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (String path : seriesPaths) {
        deleteMap.put(path, new Pair<>(startValue, endValue));
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }
  }

  /**
   * Check whether target file contain empty chunk group or not. Assert fail if it contains empty
   * chunk group whose deviceID is not in the deviceIdList.
   */
  public void check(TsFileResource targetResource, List<String> deviceIdList) throws IOException {
    byte marker;
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource.getTsFile().getAbsolutePath())) {
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            int dataSize = header.getDataSize();
            reader.position(reader.position() + dataSize);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            String deviceID = chunkGroupHeader.getDeviceID();
            if (!deviceIdList.contains(deviceID)) {
              Assert.fail(
                  "Target file "
                      + targetResource.getTsFile().getPath()
                      + " contains empty chunk group "
                      + deviceID);
            }
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
    }
  }
}
