package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceCompactionFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.RewriteCompactionFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CrossSpaceCompactionValidationTest extends AbstractCompactionTest {
    TsFileManager tsFileManager=
            new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());

    private final String oldThreadName = Thread.currentThread().getName();

    @Before
    public void setUp() throws IOException, WriteProcessException, MetadataException {
        super.setUp();
        IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
        TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
        Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
        IoTDBDescriptor.getInstance().getConfig().setCrossCompactionFileSelectionTimeBudget(-1);
    }

    @After
    public void tearDown() throws IOException, StorageEngineException {
        super.tearDown();
        Thread.currentThread().setName(oldThreadName);
        FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    }

    /**
     * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
     * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
     * Selected seq file index: 3<br>
     * Selected unseq file index: 1, 2
     */
    @Test
    public void test1() throws MetadataException, IOException, WriteProcessException, MergeException {
        registerTimeseriesInMManger(5, 10, true);
        createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
        createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
        createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
        createFiles(1, 10, 10, 1000, 5300, 5300, 100, 100, true, true);
        tsFileManager.addAll(seqResources,true);
       tsFileManager.addAll(unseqResources,false);

        CrossSpaceCompactionResource resource =
                new CrossSpaceCompactionResource(seqResources, unseqResources);
        ICrossSpaceCompactionFileSelector mergeFileSelector =
                new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
        List[] result = mergeFileSelector.select();
        Assert.assertEquals(2,result.length);
        Assert.assertEquals(2,result[0].size());
        Assert.assertEquals(2,result[1].size());

        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                result[0],
                result[1],
                IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getCrossCompactionPerformer()
                        .createInstance(),
                new AtomicInteger(0),
                tsFileManager.getNextCompactionTaskId()).doCompaction();

        validateSeqFiles();

    }

    /**
     * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
     * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
     * Selected seq file index: 3<br>
     * Selected unseq file index: 1, 2
     */
    @Test
    public void test2() throws MetadataException, IOException, WriteProcessException, MergeException {
        registerTimeseriesInMManger(5, 10, true);
        createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
        createFiles(1, 5, 10, 1000, 2100, 2100, 100, 100, true, false);
        createFiles(1, 5, 10, 1500, 3200, 3200, 100, 100, true, false);
        createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
        tsFileManager.addAll(seqResources,true);
        tsFileManager.addAll(unseqResources,false);

        CrossSpaceCompactionResource resource =
                new CrossSpaceCompactionResource(seqResources, unseqResources);
        ICrossSpaceCompactionFileSelector mergeFileSelector =
                new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
        List[] result = mergeFileSelector.select();
        Assert.assertEquals(2,result.length);
        Assert.assertEquals(1,result[0].size());
        Assert.assertEquals(2,result[1].size());

        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                result[0],
                result[1],
                IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getCrossCompactionPerformer()
                        .createInstance(),
                new AtomicInteger(0),
                tsFileManager.getNextCompactionTaskId()).doCompaction();

        validateSeqFiles();
    }

    /**
     * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
     * 4 Unseq files: 1500 ~ 2500, 1700 ~ 2000, 2400 ~ 3400, 3300 ~ 4500<br>
     * Selected seq file index: 2, 3<br>
     * Selected unseq file index: 1, 2, 3, 4
     */
    @Test
    public void test3() throws MetadataException, IOException, WriteProcessException, MergeException {
        registerTimeseriesInMManger(5, 10, true);
        createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
        createFiles(1, 5, 10, 1000, 1500, 1500, 100, 100, true, false);
        createFiles(1, 5, 10, 300, 1700, 1700, 100, 100, true, false);
        createFiles(1, 5, 10, 1000, 2400, 2400, 100, 100, true, false);
        createFiles(1, 5, 10, 1200, 3300, 3300, 100, 100, true, false);
        createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
        tsFileManager.addAll(seqResources,true);
        tsFileManager.addAll(unseqResources,false);

        CrossSpaceCompactionResource resource =
                new CrossSpaceCompactionResource(seqResources, unseqResources);
        ICrossSpaceCompactionFileSelector mergeFileSelector =
                new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
        List[] result = mergeFileSelector.select();
        Assert.assertEquals(2,result.length);
        Assert.assertEquals(2,result[0].size());
        Assert.assertEquals(4,result[1].size());

        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                result[0],
                result[1],
                IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getCrossCompactionPerformer()
                        .createInstance(),
                new AtomicInteger(0),
                tsFileManager.getNextCompactionTaskId()).doCompaction();

        validateSeqFiles();
    }

    /**
     * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6500 ~ 7500<br>
     * 5 Unseq files: 1500 ~ 2500, 1700 ~ 2000, 2400 ~ 3400, 3300 ~ 4500, 6301 ~ 7301<br>
     * Notice: the last seq file is unsealed<br>
     * Selected seq file index: 2, 3<br>
     * Selected unseq file index: 1, 2, 3, 4
     */
    @Test
    public void test4() throws MetadataException, IOException, WriteProcessException, MergeException {
        registerTimeseriesInMManger(5, 10, true);
        createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
        createFiles(1, 5, 10, 1000, 1500, 1500, 100, 100, true, false);
        createFiles(1, 5, 10, 300, 1700, 1700, 100, 100, true, false);
        createFiles(1, 5, 10, 1000, 2400, 2400, 100, 100, true, false);
        createFiles(1, 5, 10, 1200, 3300, 3300, 100, 100, true, false);
        createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
        createFiles(1, 5, 10, 1000, 6500, 6500, 100, 100, true, true);
        createFiles(1, 5, 10, 1000, 6301, 6301, 100, 100, true, false);
        seqResources.get(4).setStatus(TsFileResourceStatus.UNCLOSED);
        tsFileManager.addAll(seqResources,true);
        tsFileManager.addAll(unseqResources,false);

        CrossSpaceCompactionResource resource =
                new CrossSpaceCompactionResource(seqResources, unseqResources);
        ICrossSpaceCompactionFileSelector mergeFileSelector =
                new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
        List[] result = mergeFileSelector.select();
        Assert.assertEquals(2,result.length);
        Assert.assertEquals(2,result[0].size());
        Assert.assertEquals(4,result[1].size());

        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                result[0],
                result[1],
                IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getCrossCompactionPerformer()
                        .createInstance(),
                new AtomicInteger(0),
                tsFileManager.getNextCompactionTaskId()).doCompaction();

        validateSeqFiles();
    }

    /**
     * 7 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400, 5500 ~ 6500, 6600 ~ 7600<br>
     * 2 Unseq files: 2150 ~ 5450, 1150 ～ 5550<br>
     * Selected seq file index: 2, 3, 4, 5, 6<br>
     * Selected unseq file index: 1, 2
     */
    @Test
    public void test5() throws MetadataException, IOException, WriteProcessException, MergeException {
        registerTimeseriesInMManger(5, 10, true);
        createFiles(7, 5, 10, 1000, 0, 0, 100, 100, true, true);
        createFiles(1, 5, 10, 3300, 2150, 2150, 100, 100, true, false);
        createFiles(1, 5, 10, 4400, 1150, 1150, 100, 100, true, false);

        tsFileManager.addAll(seqResources,true);
        tsFileManager.addAll(unseqResources,false);

        CrossSpaceCompactionResource resource =
                new CrossSpaceCompactionResource(seqResources, unseqResources);
        ICrossSpaceCompactionFileSelector mergeFileSelector =
                new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
        List[] result = mergeFileSelector.select();
        Assert.assertEquals(2,result.length);
        Assert.assertEquals(5,result[0].size());
        Assert.assertEquals(2,result[1].size());

        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                result[0],
                result[1],
                IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getCrossCompactionPerformer()
                        .createInstance(),
                new AtomicInteger(0),
                tsFileManager.getNextCompactionTaskId()).doCompaction();

        validateSeqFiles();
    }

    private void validateSeqFiles(){
        List<File> files=new ArrayList<>();
        for(TsFileResource resource:tsFileManager.getTsFileList(true)){
            files.add(resource.getTsFile());
        }
        TsFileValidationTool.findUncorrectFiles(files);
        Assert.assertEquals(0,TsFileValidationTool.badFileNum);
    }

}
