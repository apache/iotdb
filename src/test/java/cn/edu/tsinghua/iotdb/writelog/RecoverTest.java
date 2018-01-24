package cn.edu.tsinghua.iotdb.writelog;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.writelog.recover.ExclusiveLogRecoverPerformer;
import cn.edu.tsinghua.iotdb.writelog.recover.RecoverPerformer;
import cn.edu.tsinghua.iotdb.writelog.replay.LogReplayer;
import cn.edu.tsinghua.iotdb.writelog.node.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static cn.edu.tsinghua.iotdb.writelog.RecoverStage.backup;
import static cn.edu.tsinghua.iotdb.writelog.RecoverStage.replayLog;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecoverTest {

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    private boolean enableWal;

    @Before
    public void setUp() throws Exception {
        enableWal = config.enableWal;
        config.enableWal = true;
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
        config.enableWal = enableWal;
    }

    @Test
    public void testFullRecover() throws IOException, RecoverException {
        // this test write a log file and try to recover from these logs as if no previous attempts exist.
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        try {
            MManager.getInstance().setStorageLevelToMTree("root.testLogNode");
        } catch (PathErrorException ignored) {
        }
        ExclusiveWriteLogNode logNode = new ExclusiveWriteLogNode("root.testLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        try {
            InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                    Arrays.asList("1.0", "15", "str", "false"));
            UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
            DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

            List<PhysicalPlan> plansToCheck = new ArrayList<>();
            plansToCheck.add(bwInsertPlan);
            plansToCheck.add(updatePlan);
            plansToCheck.add(deletePlan);

            logNode.write(bwInsertPlan);
            logNode.write(updatePlan);
            logNode.notifyStartFlush();
            logNode.write(deletePlan);
            logNode.forceSync();

            ExclusiveLogRecoverPerformer performer = new ExclusiveLogRecoverPerformer(tempRestore.getPath(), tempProcessorStore.getPath(), logNode);
            // used to check if logs are replayed in order
            DummyLogReplayer dummyLogReplayer = new DummyLogReplayer();
            dummyLogReplayer.plansToCheck = plansToCheck;
            performer.setReplayer(dummyLogReplayer);
            // used to check that FileNode does recover
            DummyFileNodeRecoverPerformer fileNodeRecoverPerformer = new DummyFileNodeRecoverPerformer();
            performer.setFileNodeRecoverPerformer(fileNodeRecoverPerformer);
            logNode.setRecoverPerformer(performer);

            logNode.recover();
            assertTrue(fileNodeRecoverPerformer.called);
            // ensure all logs are replayed
            assertEquals(plansToCheck.size(), dummyLogReplayer.currPos);

            // the log diretory should be empty now
            File logDir = new File(logNode.getLogDirectory());
            File[] files = logDir.listFiles();
            assertTrue(files == null || files.length == 0);
        } finally {
            logNode.delete();
            tempRestore.delete();
            tempProcessorStore.delete();
            tempRestore.getParentFile().delete();
        }
    }

    class DummyFileNodeRecoverPerformer implements RecoverPerformer {
        public boolean called = false;
        @Override
        public void recover() {
            called = true;
        }
    }

    class DummyLogReplayer implements LogReplayer {

        public List<PhysicalPlan> plansToCheck;
        public int currPos = 0;

        @Override
        public void replay(PhysicalPlan plan) throws ProcessorException {
            if(currPos >= plansToCheck.size())
                throw new ProcessorException("More plans recovered than expected");
            assertEquals(plansToCheck.get(currPos++), plan);
        }
    }

    @Test
    public void testRecoverFromRecoverFiles() throws IOException, RecoverException {
        // this test write a log file and try to recover from these logs as if a previous attempt is interrupted when recovering files or replaying logs.
        // skip file backup by setting backup flag and creating backup files.
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        File tempRestoreRecovery = new File("testtemp", "restore" + ExclusiveLogRecoverPerformer.RECOVER_SUFFIX);
        File tempProcessorStoreRecovery = new File("testtemp", "processorStore"+ ExclusiveLogRecoverPerformer.RECOVER_SUFFIX);
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();
        tempRestoreRecovery.createNewFile();
        tempProcessorStoreRecovery.createNewFile();

        try {
            MManager.getInstance().setStorageLevelToMTree("root.testLogNode");
        } catch (PathErrorException ignored) {
        }
        ExclusiveWriteLogNode logNode = new ExclusiveWriteLogNode("root.testLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        try {
            // set flag
            File flagFile = new File(logNode.getLogDirectory() + File.separator + ExclusiveLogRecoverPerformer.RECOVER_FLAG_NAME + "-" + backup.name());
            flagFile.createNewFile();

            InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                    Arrays.asList("1.0", "15", "str", "false"));
            UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
            DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

            List<PhysicalPlan> plansToCheck = new ArrayList<>();
            plansToCheck.add(bwInsertPlan);
            plansToCheck.add(updatePlan);
            plansToCheck.add(deletePlan);

            logNode.write(bwInsertPlan);
            logNode.write(updatePlan);
            logNode.write(deletePlan);
            logNode.forceSync();

            ExclusiveLogRecoverPerformer performer = new ExclusiveLogRecoverPerformer(tempRestore.getPath(), tempProcessorStore.getPath(), logNode);
            // used to check if logs are replayed in order
            DummyLogReplayer dummyLogReplayer = new DummyLogReplayer();
            dummyLogReplayer.plansToCheck = plansToCheck;
            performer.setReplayer(dummyLogReplayer);
            // used to check that FileNode does recover
            DummyFileNodeRecoverPerformer fileNodeRecoverPerformer = new DummyFileNodeRecoverPerformer();
            performer.setFileNodeRecoverPerformer(fileNodeRecoverPerformer);
            logNode.setRecoverPerformer(performer);

            logNode.recover();
            assertTrue(fileNodeRecoverPerformer.called);
            // ensure all logs are replayed
            assertEquals(plansToCheck.size(), dummyLogReplayer.currPos);

            // the log diretory should be empty now
            File logDir = new File(logNode.getLogDirectory());
            File[] files = logDir.listFiles();
            assertTrue(files == null || files.length == 0);
        } finally {
            logNode.delete();
            tempRestore.delete();
            tempProcessorStore.delete();
            assertTrue(!tempRestoreRecovery.exists());
            assertTrue(!tempProcessorStoreRecovery.exists());
            tempRestore.getParentFile().delete();
        }
    }

    @Test
    public void testRecoverFromCleanup() throws IOException, RecoverException {
        // this test write a log file and try to recover from these logs as if a previous attempt is interrupted when cleanup files.
        // skip previous stage by setting backup flag and creating backup files.
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        File tempRestoreRecovery = new File("testtemp", "restore" + ExclusiveLogRecoverPerformer.RECOVER_SUFFIX);
        File tempProcessorStoreRecovery = new File("testtemp", "processorStore"+ ExclusiveLogRecoverPerformer.RECOVER_SUFFIX);
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();
        tempRestoreRecovery.createNewFile();
        tempProcessorStoreRecovery.createNewFile();

        try {
            MManager.getInstance().setStorageLevelToMTree("root.testLogNode");
        } catch (PathErrorException ignored) {
        }
        ExclusiveWriteLogNode logNode = new ExclusiveWriteLogNode("root.testLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        try {
            // set flag
            File flagFile = new File(logNode.getLogDirectory() + File.separator + ExclusiveLogRecoverPerformer.RECOVER_FLAG_NAME + "-" + replayLog.name());
            flagFile.createNewFile();

            InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                    Arrays.asList("1.0", "15", "str", "false"));
            UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
            DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

            List<PhysicalPlan> plansToCheck = new ArrayList<>();
            plansToCheck.add(bwInsertPlan);
            plansToCheck.add(updatePlan);
            plansToCheck.add(deletePlan);

            logNode.write(bwInsertPlan);
            logNode.write(updatePlan);
            logNode.write(deletePlan);
            logNode.forceSync();

            ExclusiveLogRecoverPerformer performer = new ExclusiveLogRecoverPerformer(tempRestore.getPath(), tempProcessorStore.getPath(), logNode);
            // used to check that no log is replayed
            DummyLogReplayer dummyLogReplayer = new DummyLogReplayer();
            performer.setReplayer(dummyLogReplayer);
            // used to check that FileNode does recover
            DummyFileNodeRecoverPerformer fileNodeRecoverPerformer = new DummyFileNodeRecoverPerformer();
            performer.setFileNodeRecoverPerformer(fileNodeRecoverPerformer);
            logNode.setRecoverPerformer(performer);

            logNode.recover();
            assertTrue(!fileNodeRecoverPerformer.called);

            // the log diretory should be empty now
            File logDir = new File(logNode.getLogDirectory());
            File[] files = logDir.listFiles();
            assertTrue(files == null || files.length == 0);
        } finally {
            logNode.delete();
            tempRestore.delete();
            tempProcessorStore.delete();
            assertTrue(!tempRestoreRecovery.exists());
            assertTrue(!tempProcessorStoreRecovery.exists());
            tempRestore.getParentFile().delete();
        }
    }
}
