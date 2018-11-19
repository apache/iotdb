package cn.edu.tsinghua.iotdb.qp.other;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.sys.AuthorPlan;
import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

/**
 * test ast node parsing on authorization
 * 
 * @author kangrong
 *
 */
@RunWith(Parameterized.class)
public class TSPlanContextAuthorTest {
    private static Path[] emptyPaths = new Path[] {};
    private static Path[] testPaths = new Path[] {new Path("root.node1.a.b")};

    private String inputSQL;
    private Path[] paths;


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"CREATE USER username1 password1", emptyPaths},
                {"DROP USER username", emptyPaths},
                {"CREATE ROLE rolename", emptyPaths},
                {"DROP ROLE rolename", emptyPaths},
                {"GRANT USER username PRIVILEGES 'SET_STORAGE_GROUP','INSERT_TIMESERIES' ON root.node1.a.b", testPaths},
                {"REVOKE USER username PRIVILEGES 'SET_STORAGE_GROUP','INSERT_TIMESERIES' ON root.node1.a.b", testPaths},
                {"GRANT ROLE rolename PRIVILEGES 'SET_STORAGE_GROUP','INSERT_TIMESERIES' ON root.node1.a.b", testPaths},
                {"REVOKE ROLE rolename PRIVILEGES 'SET_STORAGE_GROUP','INSERT_TIMESERIES' ON root.node1.a.b", testPaths},
                {"GRANT rolename TO username", emptyPaths},
                {"REVOKE rolename FROM username", emptyPaths}});
    }

    public TSPlanContextAuthorTest(String inputSQL, Path[] paths) {
        this.inputSQL = inputSQL;
        this.paths = paths;
    }

    @Test
    public void testAnalyzeAuthor() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        AuthorPlan author = (AuthorPlan) processor.parseSQLToPhysicalPlan(inputSQL);
        if (author == null)
            fail();
        assertArrayEquals(paths, author.getPaths().toArray());
    }

}
