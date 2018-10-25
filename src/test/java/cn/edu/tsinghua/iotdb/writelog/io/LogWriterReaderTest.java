package cn.edu.tsinghua.iotdb.writelog.io;

import cn.edu.tsinghua.iotdb.exception.WALOverSizedException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.writelog.io.LogWriter;
import cn.edu.tsinghua.iotdb.writelog.transfer.PhysicalPlanCodec;
import cn.edu.tsinghua.iotdb.writelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.writelog.transfer.SystemLogOperator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogWriterReaderTest {
    List<byte[]> logs=new ArrayList<>();
    List<PhysicalPlan> plans=new ArrayList<>();
    private static String filePath="logtest.test";
    @Before
    public void prepare() throws WALOverSizedException {
        if(new File(filePath).exists()) new File(filePath).delete();
        InsertPlan insertPlan1=new InsertPlan(1, "d1", 10L, Arrays.asList("s1", "s2"), Arrays.asList("1", "2"));
        InsertPlan insertPlan2=new InsertPlan(2, "d1", 10L, Arrays.asList("s1", "s2"), Arrays.asList("1", "2"));
        UpdatePlan updatePlan=new UpdatePlan(8L, 11L, "3", new Path("root.d1.s1"));
        DeletePlan deletePlan=new DeletePlan(10L, new Path("root.d1.s1"));
        plans.add(insertPlan1);
        plans.add(insertPlan2);
        plans.add(updatePlan);
        plans.add(deletePlan);
        for(PhysicalPlan plan:plans){
            logs.add(PhysicalPlanLogTransfer.operatorToLog(plan));

        }
    }

    @Test
    public void testWriteAndRead() throws IOException {
        LogWriter writer=new LogWriter(filePath);
        writer.write(logs);
        try {
            writer.close();
            RAFLogReader reader=new RAFLogReader(new File(filePath));
            List<byte[]> res=new ArrayList<>();
            while (reader.hasNext()){
                res.add(PhysicalPlanLogTransfer.operatorToLog(reader.next()));
            }
            for(int i=0; i< logs.size(); i++){
                assertArrayEquals(logs.get(i),res.get(i));
            }
             reader.close();
        }finally {
            new File(filePath).delete();
        }
    }
}
