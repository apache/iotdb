package cn.edu.thu.tsfiledb.sys.writeLog;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.physical.plan.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.InsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.UpdatePlan;
import org.junit.Test;

import java.io.IOException;

/**
 * @author CGF.
 */
public class WriteLogNodeTest {

    private static String device, sensor;

    @Test
    public void bufferWriteOverflowFlushTest1() throws IOException {
        WriteLogNode node = new WriteLogNode(new Path("d1.s1"));
        node.resetFileStatus();
        node.write(new InsertPlan(1, 100L, "1.0", new Path("root.vehicle")));
        node.write(new UpdatePlan(200L, 300L, "2.0", new Path("root.vehicle")));
        node.write(new DeletePlan(200L, new Path("root.vehicle")));
        node.write(new UpdatePlan(400L, 500L, "3.0", new Path("root.vehicle")));
        node.write(new UpdatePlan(500L, 600L, "4.0", new Path("root.vehicle")));
        PhysicalPlan plan = null;
        node.bufferFlushStart();
        node.write(new UpdatePlan(900L, 901L, "3.0", new Path("root.vehicle")));
        node.bufferFlushEnd();
        node.write(new InsertPlan(1, 101L, "4.0", new Path("root.vehicle")));
        node.write(new InsertPlan(2, 105L, "8.0", new Path("root.vehicle")));
        node.overflowFlushStart();
        node.write(new UpdatePlan(500L, 600L, "4.0", new Path("root.vehicle")));
        node.overflowFlushEnd();
        System.out.println("-----");
        while ((plan = node.getPhysicalPlan()) != null) {
            output(plan);
        }
    }

    @Test
    public void MergeTest1() throws IOException {
        WriteLogNode node = new WriteLogNode(new Path("d1.s1"));
        node.resetFileStatus();
        node.write(new InsertPlan(1, 100L, "1.0", new Path("root.vehicle")));
        node.write(new UpdatePlan(200L, 300L, "2.0", new Path("root.vehicle")));
        node.write(new DeletePlan(200L, new Path("root.vehicle")));
        node.write(new UpdatePlan(400L, 500L, "3.0", new Path("root.vehicle")));
        node.write(new UpdatePlan(500L, 600L, "4.0", new Path("root.vehicle")));
        PhysicalPlan plan = null;
        node.bufferFlushStart();
        node.write(new UpdatePlan(900L, 901L, "3.0", new Path("root.vehicle")));
        node.bufferFlushEnd();
        node.write(new InsertPlan(1, 101L, "4.0", new Path("root.vehicle")));
        node.write(new InsertPlan(2, 105L, "8.0", new Path("root.vehicle")));
        node.overflowFlushStart();
        node.write(new UpdatePlan(500L, 600L, "4.0", new Path("root.vehicle")));
        node.overflowFlushEnd();
        System.out.println("-----");
        while ((plan = node.getPhysicalPlan()) != null) {
            output(plan);
        }
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
