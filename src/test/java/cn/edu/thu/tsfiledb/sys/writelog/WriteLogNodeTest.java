package cn.edu.thu.tsfiledb.sys.writelog;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.InsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.UpdatePlan;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author CGF.
 */
public class WriteLogNodeTest {

    private static Path path = new Path("d1.s1");
    private static String fileNode = "root.vehicle.d1";
    private List<String> measurements = new ArrayList<>();
    private List<String> values = new ArrayList<>();
    
    
    @After
    public void tearDown(){
    	TsfileDBConfig dbConfig = TsfileDBDescriptor.getInstance().getConfig();
    	EngineTestHelper.delete(dbConfig.walFolder);
    }

    @Test
    public void bufferWriteOverflowFlushTest() throws IOException {
        WriteLogNode node = new WriteLogNode(fileNode);
        measurements.clear();
        measurements.add("s1");
        values.add("1.0");
        node.write(new InsertPlan("d1", 100L, measurements, values));
        node.write(new UpdatePlan(200L, 300L, "2.0", path));
        node.write(new DeletePlan(200L, path));
        node.write(new UpdatePlan(400L, 500L, "3.0", path));
        node.write(new UpdatePlan(500L, 600L, "4.0", path));
        node.bufferFlushStart();
        node.write(new UpdatePlan(900L, 901L, "3.0", path));
        node.bufferFlushEnd();
        values.clear();
        values.add("4.0");
        node.write(new InsertPlan(1, "d1", 101L, measurements, values));
        node.overflowFlushStart();
        node.write(new UpdatePlan(500L, 600L, "4.0", path));
        node.overflowFlushEnd();
        int cnt = 1;
        PhysicalPlan plan;
        while ((plan = node.getPhysicalPlan()) != null) {
            if (cnt == 1) {
                Assert.assertEquals(plan.getPaths().get(0), path);
                Assert.assertTrue(plan instanceof InsertPlan);
                InsertPlan insertPlan = (InsertPlan) plan;
                Assert.assertEquals(insertPlan.getTime(), 101L);
                Assert.assertEquals(insertPlan.getValues().get(0), "4.0");
            } else if (cnt == 2) {
                Assert.assertEquals(plan.getPaths().get(0), path);
                Assert.assertTrue(plan instanceof UpdatePlan);
                UpdatePlan updatePlan = (UpdatePlan) plan;
                Assert.assertEquals(updatePlan.getStartTime(), 500L);
                Assert.assertEquals(updatePlan.getEndTime(), 600L);
                Assert.assertEquals(updatePlan.getValue(), "4.0");
            }
            cnt++;
            //output(plan);
        }
        node.closeStreams();
        node.removeFiles();
    }

    @Test
    public void logMemorySizeTest() throws IOException {
        measurements.clear();
        measurements.add("s1");
        WriteLogNode node = new WriteLogNode(fileNode);
        node.setLogMemorySize(100);
        for (int i = 1; i <= 99; i++) {
            node.write(new UpdatePlan(i, i * 2, "1.0", path));
        }
        PhysicalPlan plan = node.getPhysicalPlan();
        Assert.assertTrue(plan == null);
        node.readerReset();

        values.clear();
        values.add("1.0");
        node.write(new InsertPlan(1, fileNode, 100L, measurements, values));
        for (int i = 101; i <= 201; i++) {
            node.write(new UpdatePlan(i, i * 2, "2.0", path));
        }
        int cnt = 1;
        while ((plan = node.getPhysicalPlan()) != null) {
            if (cnt == 1) {
                Assert.assertEquals(plan.getPaths().get(0), path);
                Assert.assertTrue(plan instanceof UpdatePlan);
                UpdatePlan updatePlan = (UpdatePlan) plan;
                Assert.assertEquals(updatePlan.getStartTime(), 1L);
                Assert.assertEquals(updatePlan.getEndTime(), 2L);
                Assert.assertEquals(updatePlan.getValue(), "1.0");
            } else if (cnt == 100) {
                Assert.assertEquals(plan.getPaths().get(0), new Path("root.vehicle.d1.s1"));
                Assert.assertTrue(plan instanceof InsertPlan);
                InsertPlan insertPlan = (InsertPlan) plan;
                Assert.assertEquals(insertPlan.getTime(), 100L);
                Assert.assertEquals(insertPlan.getValues().get(0), "1.0");
            } else if (cnt == 200) {
                Assert.assertEquals(plan.getPaths().get(0), path);
                Assert.assertTrue(plan instanceof UpdatePlan);
                UpdatePlan updatePlan = (UpdatePlan) plan;
                Assert.assertEquals(updatePlan.getStartTime(), 200L);
                Assert.assertEquals(updatePlan.getEndTime(), 400L);
                Assert.assertEquals(updatePlan.getValue(), "2.0");
            }
            cnt++;
        }
        Assert.assertEquals(cnt, 201);
        node.closeStreams();
        node.removeFiles();
    }

    @Test
    public void logCompactTest() throws IOException {

        // OverflowFlush Start + OverflowFlush End;
        WriteLogNode node = new WriteLogNode(fileNode);
        node.setLogMemorySize(10);
        node.setLogCompactSize(100);
        for (int i = 1; i <= 100; i++) {
            node.write(new UpdatePlan(i, i * 2, "1.0", path));
        }
        for (int i = 101; i <= 200; i++) {
            node.write(new UpdatePlan(i, i * 2, "2.0", path));
        }
        node.overflowFlushStart();
        node.overflowFlushEnd();
        PhysicalPlan plan;
        int cnt = 0;
        while ((plan = node.getPhysicalPlan()) != null) {
            cnt++;
            output(plan);
        }
        Assert.assertEquals(cnt, 0);
        node.readerReset();

        List<String> measurementList = new ArrayList<>();
        List<String> valueList = new ArrayList<>();
        for (int i = 0; i <= 100; i++) {
            measurementList.add("s0");
            valueList.add(String.valueOf(i));
        }
        InsertPlan InsertPlan = new InsertPlan(1, fileNode, 1L, measurementList, valueList);
        node.write(InsertPlan);
        node.bufferFlushStart();
        node.bufferFlushEnd();
        for (int i = 300; i <= 500; i++) {
            if (i == 409) {
                node.overflowFlushStart();
            }
            if (i == 470) {
                node.overflowFlushEnd();
            }
            node.write(new UpdatePlan(i, i * 2, "8.0", path));
        }
        cnt = 0;
        while ((plan = node.getPhysicalPlan()) != null) {
            cnt++;
            // output(plan);
        }
        Assert.assertEquals(cnt, 91);
        node.readerReset();

        cnt = 0;
        node.setLogMemorySize(1);
        node.setLogCompactSize(10);
        for (int i = 1; i <= 10; i++) {
            measurementList = new ArrayList<>();
            valueList = new ArrayList<>();
            for (int j = 1; j <= 10; j++) {
                measurementList.add("s" + i + "-" + j);
                valueList.add(String.valueOf(i));
            }
            InsertPlan = new InsertPlan(1, fileNode, 1L, measurementList, valueList);
            node.write(InsertPlan);
        }
        node.bufferFlushStart();
        node.bufferFlushEnd();
        for (int i = 1; i <= 1; i++) {
            measurementList = new ArrayList<>();
            valueList = new ArrayList<>();
            for (int j = 1; j <= 10; j++) {
                measurementList.add("s" + i + "-" + j);
                valueList.add(String.valueOf(i));
            }
            InsertPlan = new InsertPlan(1, fileNode, 1L, measurementList, valueList);
            node.write(InsertPlan);
        }
        while ((plan = node.getPhysicalPlan()) != null) {
            cnt++;
            //output(plan);
        }
        Assert.assertEquals(cnt, 93);
        node.closeStreams();
        node.removeFiles();
    }

    @Test
    public void multiInsertTest() throws IOException {
        WriteLogNode node = new WriteLogNode(fileNode);
        node.setLogMemorySize(1);
        node.setLogCompactSize(100);
        List<String> measurementList = new ArrayList<>();
        List<String> valueList = new ArrayList<>();

        for (int i = 0; i <= 10000; i++) {
            measurementList.add("s0");
            valueList.add(String.valueOf(i));
        }
        InsertPlan InsertPlan = new InsertPlan(1, fileNode, 1L, measurementList, valueList);
        node.write(InsertPlan);

        PhysicalPlan plan;
        while ((plan = node.getPhysicalPlan()) != null) {
            //InsertPlan = (InsertPlan) plan;
            Assert.assertEquals(InsertPlan.getMeasurements().size(), 10001);
            //output(plan);
        }
        node.closeStreams();
        node.removeFiles();
    }

//    @Test
    public void systemLogTimingMergingTest() throws IOException, InterruptedException {
        WriteLogNode node = new WriteLogNode(fileNode);
        measurements.clear();
        measurements.add("s1");
        values.add("1.0");
        node.write(new InsertPlan("d1", 100L, measurements, values));
        node.write(new UpdatePlan(200L, 300L, "2.0", path));
        node.write(new DeletePlan(200L, path));
        node.write(new UpdatePlan(400L, 500L, "3.0", path));
        node.write(new UpdatePlan(500L, 600L, "4.0", path));
        node.write(new UpdatePlan(900L, 901L, "3.0", path));
        values.clear();
        values.add("4.0");
        node.write(new InsertPlan(1, "d1", 101L, measurements, values));
        node.write(new UpdatePlan(500L, 600L, "4.0", path));
        Thread.sleep(3000);
        int cnt = 0;
        PhysicalPlan plan;
        while ((plan = node.getPhysicalPlan()) != null) {
            // output(plan);
            cnt ++;
        }
        Assert.assertEquals(0, cnt);
        node.closeStreams();
        node.removeFiles();
    }

    //@Test
    public void recoveryTest() {

    }

    private void output(PhysicalPlan plan) {
        if (plan instanceof UpdatePlan) {
            UpdatePlan p = (UpdatePlan) plan;
            System.out.println("Update: " + p.getPath() + " " + p.getStartTime() + " " + p.getEndTime() + " " + p.getValue());
        } else if (plan instanceof DeletePlan) {
            DeletePlan p = (DeletePlan) plan;
            System.out.println("Delete: " + p.getPaths().get(0) + " " + p.getDeleteTime());
        } else if (plan instanceof InsertPlan) {
            InsertPlan InsertPlan = (InsertPlan) plan;
            System.out.println("MultiInsert: " + InsertPlan.getDeltaObject() + " " + InsertPlan.getTime());
            for (int i = 0; i < InsertPlan.getMeasurements().size(); i++) {
                System.out.println(InsertPlan.getMeasurements().get(i) + " " + InsertPlan.getValues().get(i));
            }
        }
    }
}
