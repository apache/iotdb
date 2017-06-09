package cn.edu.thu.tsfiledb.sql.exec.query;


import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.exec.impl.SingleFileQPExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.fail;


/**
 * test query operation
 * 
 * @author kangrong
 *
 */
@RunWith(Parameterized.class)
public class TestSingleFileQpQuery {
    private static final Logger LOG = LoggerFactory.getLogger(TestSingleFileQpQuery.class);
    private QueryProcessExecutor exec;

    @Before
    public void before() throws IOException {
        File file = new File("src/test/resources/testMultiDeviceMerge.tsfile");
        if(!file.exists())
            exec = null;
        else
            exec =
                new SingleFileQPExecutor(new LocalFileInput(
                        "src/test/resources/testMultiDeviceMerge.tsfile"));

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
    public void testQueryBasic() throws QueryProcessorException {
        if(exec == null)
            return;
        LOG.info("input SQL String:{}", inputSQL);
        TSqlParserV2 parser = new TSqlParserV2();
        RootOperator root = parser.parseSQLToOperator(inputSQL);
        if (!root.isQuery())
            fail();
        Iterator<QueryDataSet> iter = parser.query(root, exec);
        LOG.info("query result:");
        int i = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                if (i == result.length)
                    fail();
                String actual = set.getNextRecord().toString();
                // System.out.println(actual);
                LOG.info("freq data:{}", actual);
//                 assertEquals(result[i++], actual);
            }
        }
        LOG.info("Query processing complete\n");
    }
}
