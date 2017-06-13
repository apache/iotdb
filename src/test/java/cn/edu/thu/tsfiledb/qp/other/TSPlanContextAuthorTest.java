package cn.edu.thu.tsfiledb.qp.other;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.physical.sys.AuthorPlan;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * test ast node parsing on authorization
 * 
 * @author kangrong
 *
 */
@RunWith(Parameterized.class)
public class TSPlanContextAuthorTest {
    private static Path[] emptyPaths = new Path[] {};
    private static Path[] testPaths = new Path[] {new Path("node1.a.b")};

    private String inputSQL;
    private Path[] paths;


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"CREATE USER username1 password1", emptyPaths},
                {"DROP USER username", emptyPaths},
                {"CREATE ROLE rolename", emptyPaths},
                {"DROP ROLE rolename", emptyPaths},
                {"GRANT USER username PRIVILEGES 'create','insert' ON node1.a.b", testPaths},
                {"REVOKE USER username PRIVILEGES 'create','insert' ON node1.a.b", testPaths},
                {"GRANT ROLE rolename PRIVILEGES 'create','insert' ON node1.a.b", testPaths},
                {"REVOKE ROLE rolename PRIVILEGES 'create','insert' ON node1.a.b", testPaths},
                {"GRANT rolename TO username", emptyPaths},
                {"REVOKE rolename FROM username", emptyPaths}});
    }

    public TSPlanContextAuthorTest(String inputSQL, Path[] paths) {
        this.inputSQL = inputSQL;
        this.paths = paths;
    }

    @Test
    public void testanalyzeAuthor() throws QueryProcessorException {
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        AuthorPlan author = (AuthorPlan) processor.parseSQLToPhysicalPlan(inputSQL);
        if (author == null)
            fail();
        assertArrayEquals(paths, author.getPaths().toArray());
    }

}
