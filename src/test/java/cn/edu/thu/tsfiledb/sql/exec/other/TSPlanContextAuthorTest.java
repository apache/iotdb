package cn.edu.thu.tsfiledb.sql.exec.other;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.author.AuthorOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.AuthorPlan;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
    private static String[] defaultPrivilegeList = {"create", "insert"};
    private static Path defaultNameNode = new Path("node1.a.b");

    private String inputSQL;
    private String tokenName;
    private String roleName;
    private String username;
    private String password;
    private String[] privilegeList;
    private Path nameNode;
    private Path[] paths;


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"CREATE USER username1 password1", "TOK_AUTHOR_CREATE", null, "username1",
                        "password1", null, null, emptyPaths},
                {"DROP USER username", "TOK_AUTHOR_DROP", null, "username", null, null, null,
                        emptyPaths},
                {"CREATE ROLE rolename", "TOK_AUTHOR_CREATE", "rolename", null, null, null, null,
                        emptyPaths},
                {"DROP ROLE rolename", "TOK_AUTHOR_DROP", "rolename", null, null, null, null,
                        emptyPaths},
                {"GRANT USER username PRIVILEGES 'create','insert' ON node1.a.b",
                        "TOK_AUTHOR_GRANT", null, "username", null, defaultPrivilegeList,
                        defaultNameNode, testPaths},
                {"REVOKE USER username PRIVILEGES 'create','insert' ON node1.a.b",
                        "TOK_AUTHOR_REVOKE", null, "username", null, defaultPrivilegeList,
                        defaultNameNode, testPaths},
                {"GRANT ROLE rolename PRIVILEGES 'create','insert' ON node1.a.b",
                        "TOK_AUTHOR_GRANT", "rolename", null, null, defaultPrivilegeList,
                        defaultNameNode, testPaths},
                {"REVOKE ROLE rolename PRIVILEGES 'create','insert' ON node1.a.b",
                        "TOK_AUTHOR_REVOKE", "rolename", null, null, defaultPrivilegeList,
                        defaultNameNode, testPaths},
                {"GRANT rolename TO username", "TOK_AUTHOR_GRANT", "rolename", "username", null,
                        null, null, emptyPaths},
                {"REVOKE rolename FROM username", "TOK_AUTHOR_REVOKE", "rolename", "username",
                        null, null, null, emptyPaths}});
    }

    public TSPlanContextAuthorTest(String inputSQL, String tokenName, String roleName,
            String username, String password, String[] privilegeList, Path nameNode, Path[] paths) {
        this.inputSQL = inputSQL;
        this.tokenName = tokenName;
        this.roleName = roleName;
        this.username = username;
        this.password = password;
        this.privilegeList = privilegeList;
        this.nameNode = nameNode;
        this.paths = paths;
    }

    @Test
    public void testanalyzeAuthor() throws QueryProcessorException {
        TSqlParserV2 parser = new TSqlParserV2();
        AuthorOperator author = (AuthorOperator) parser.parseSQLToOperator(this.inputSQL);
        if (author == null)
            fail();
        AuthorPlan plan = (AuthorPlan) parser.transformToPhysicalPlan(author, null);
        assertEquals(tokenName, author.getTokenName());
        assertEquals(roleName, author.getRoleName());
        assertEquals(username, author.getUserName());
        assertEquals(password, author.getPassWord());
        assertArrayEquals(privilegeList, author.getPrivilegeList());
        assertEquals(nameNode, author.getNodeName());
        assertArrayEquals(paths, plan.getPaths().toArray());
    }

}
