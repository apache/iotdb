package cn.edu.thu.tsfiledb.sys.writeLog;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.physical.plan.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.InsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.UpdatePlan;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author CGF.
 */
public class WriteLogNodeTest {

    private static String device, sensor;
    private static Path path = new Path("d1.s1");
    private static String fileNode = "root.vehicle.d1";

    @Test
    public void bufferWriteOverflowFlushTest() throws IOException {
        WriteLogNode node = new WriteLogNode(fileNode);
        node.resetFileStatus();
        node.write(new InsertPlan(1, 100L, "1.0", path));
        node.write(new UpdatePlan(200L, 300L, "2.0", path));
        node.write(new DeletePlan(200L, new Path("root.vehicle")));
        node.write(new UpdatePlan(400L, 500L, "3.0", path));
        node.write(new UpdatePlan(500L, 600L, "4.0", path));
        node.bufferFlushStart();
        node.write(new UpdatePlan(900L, 901L, "3.0", path));
        node.bufferFlushEnd();
        node.write(new InsertPlan(1, 101L, "4.0", path));
        node.write(new InsertPlan(2, 105L, "8.0", path));
        node.overflowFlushStart();
        node.write(new UpdatePlan(500L, 600L, "4.0", path));
        node.overflowFlushEnd();
        int cnt = 1;
        PhysicalPlan plan;
        while ((plan = node.getPhysicalPlan()) != null) {
            if (cnt == 1) {
                Assert.assertEquals(plan.getPath(), path);
                Assert.assertTrue(plan instanceof InsertPlan);
                InsertPlan insertPlan = (InsertPlan) plan;
                Assert.assertEquals(insertPlan.getTime(), 101L);
                Assert.assertEquals(insertPlan.getValue(), "4.0");
            } else if (cnt == 2) {
                Assert.assertEquals(plan.getPath(), path);
                Assert.assertTrue(plan instanceof UpdatePlan);
                UpdatePlan updatePlan = (UpdatePlan) plan;
                Assert.assertEquals(updatePlan.getStartTime(), 500L);
                Assert.assertEquals(updatePlan.getEndTime(), 600L);
                Assert.assertEquals(updatePlan.getValue(), "4.0");
            }
            cnt++;
        }
    }

    @Test
    public void logMemorySizeTest() throws IOException {
        WriteLogNode node = new WriteLogNode(fileNode);
        node.resetFileStatus();
        node.setLogMemorySize(100);
        for (int i = 1; i <= 99; i++) {
            node.write(new UpdatePlan(i, i * 2, "1.0", path));
        }
        PhysicalPlan plan = node.getPhysicalPlan();
        Assert.assertTrue(plan == null);
        node.write(new InsertPlan(1, 100L, "1.0", path));
        for (int i = 101; i <= 201; i++) {
            node.write(new UpdatePlan(i, i * 2, "2.0", path));
        }
        int cnt = 1;
        while ((plan = node.getPhysicalPlan()) != null) {
            if (cnt == 1) {
                Assert.assertEquals(plan.getPath(), path);
                Assert.assertTrue(plan instanceof UpdatePlan);
                UpdatePlan updatePlan = (UpdatePlan) plan;
                Assert.assertEquals(updatePlan.getStartTime(), 1L);
                Assert.assertEquals(updatePlan.getEndTime(), 2L);
                Assert.assertEquals(updatePlan.getValue(), "1.0");
            } else if (cnt == 100) {
                Assert.assertEquals(plan.getPath(), path);
                Assert.assertTrue(plan instanceof InsertPlan);
                InsertPlan insertPlan = (InsertPlan) plan;
                Assert.assertEquals(insertPlan.getTime(), 100L);
                Assert.assertEquals(insertPlan.getValue(), "1.0");
            } else if (cnt == 200) {
                Assert.assertEquals(plan.getPath(), path);
                Assert.assertTrue(plan instanceof UpdatePlan);
                UpdatePlan updatePlan = (UpdatePlan) plan;
                Assert.assertEquals(updatePlan.getStartTime(), 200L);
                Assert.assertEquals(updatePlan.getEndTime(), 400L);
                Assert.assertEquals(updatePlan.getValue(), "2.0");
            }
            cnt++;
            // output(plan);
        }
        Assert.assertEquals(cnt, 201);
    }

    @Test
    public void logCompactTest() throws IOException {
        WriteLogNode node = new WriteLogNode(fileNode);
        node.resetFileStatus();
        node.setLogMemorySize(10);
        node.setLogCompactSize(100);
        for (int i = 1; i <= 100; i++) {
            node.write(new UpdatePlan(i, i * 2, "1.0", path));
        }
        for (int i = 101; i <= 200; i++) {
            node.write(new UpdatePlan(i, i * 2, "2.0", path));
        }
        //node.write(new InsertPlan(1, 300L, "3.0", path));
        node.bufferFlushStart();
        node.bufferFlushEnd();
        PhysicalPlan plan = null;
        while ((plan = node.getPhysicalPlan()) != null) {
            // cnt++;
            output(plan);
        }
    }

    @Test
    public void recoveryTest() {

    }

    private void output(PhysicalPlan plan) {
        if (plan instanceof InsertPlan) {
            InsertPlan p = (InsertPlan) plan;
            System.out.println("Insert: " + p.getPath() + " " + p.getTime() + " " + p.getValue());
        } else if (plan instanceof UpdatePlan) {
            UpdatePlan p = (UpdatePlan) plan;
            System.out.println("Update: " + p.getPath() + " " + p.getStartTime() + " " + p.getEndTime() + " " + p.getValue());
        } else if (plan instanceof DeletePlan) {
            DeletePlan p = (DeletePlan) plan;
            System.out.println("Delete:" + p.getPath() + " " + p.getDeleteTime());
        }
    }
}
