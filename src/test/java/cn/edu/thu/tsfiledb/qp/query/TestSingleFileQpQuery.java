package cn.edu.thu.tsfiledb.qp.query;

import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.executor.SingleFileQPExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * test query operation
 * 
 * @author kangrong
 *
 */
@RunWith(Parameterized.class)
public class TestSingleFileQpQuery {
    private QueryProcessExecutor exec;
    private QueryProcessor processor;

    @Before
    public void before() throws IOException {
        File file = new File("src/test/resources/testMultiDeviceMerge.tsfile");
        if(!file.exists())
            exec = null;
        else {
            exec = new SingleFileQPExecutor(new LocalFileInput("src/test/resources/testMultiDeviceMerge.tsfile"));
            processor = new QueryProcessor(exec);
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {
                "select root.laptop.d0.s0,root.laptop.d0.s1,root.laptop.d0.s2 where time > 0 and root.laptop.d0.s1 > 0",
                new String[] {"20, <device_1.sensor_1,21> <device_1.sensor_2,null> ",
                        "50, <device_1.sensor_1,null> <device_1.sensor_2,52> ",
                        "100, <device_1.sensor_1,101> <device_1.sensor_2,102> "}
                }
        });
    }

    private final String inputSQL;
    private final String[] result;

    public TestSingleFileQpQuery(String inputSQL, String[] result) {
        this.inputSQL = inputSQL;
        this.result = result;
    }

    @Test
    public void testQueryBasic() throws QueryProcessorException, ArgsErrorException {
        if(exec == null)
            return;
        PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(inputSQL);
        if (!physicalPlan.isQuery())
            fail();
        Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(physicalPlan);
        int i = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                if (i == result.length)
                    fail();
                String actual = set.getNextRecord().toString();
                assertEquals(result[i++], actual);
            }
        }
    }
}
