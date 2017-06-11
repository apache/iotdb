package cn.edu.thu.tsfiledb.sql.exec.other;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.metadata.MetadataOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.metadata.MetadataOperator.NamespaceType;
import cn.edu.thu.tsfiledb.qp.physical.plan.metadata.MetadataPlan;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import org.antlr.runtime.RecognitionException;
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
public class TSPlanContextMetadataTest extends TSqlParserV2 {
    private static Path[] emptyPaths = new Path[] {};
    private static Path[] testPaths = new Path[] {new Path("root.laptop.d1.s1")};
    private static Path defaultPath = new Path("root.laptop.d1.s1");
    
    private String inputSQL;
    private NamespaceType namespaceType;
    private Path path;
    private String dataType;
    private String encoding;
    private String[] encodingArgs;
    private Path[] paths;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"create timeseries root.laptop.d1.s1 with datatype=INT32,encoding=RLE,rea=asd",
                        NamespaceType.ADD_PATH, defaultPath, "INT32", "RLE",
                        new String[] {"rea=asd"}, testPaths},
                {"DELETE TIMESERIES root.laptop.d1.s1", NamespaceType.DELETE_PATH,
                            defaultPath, null, null, null, testPaths},
                {"SET STORAGE GROUP TO root.laptop.d1.s1", NamespaceType.SET_FILE_LEVEL,
                                defaultPath, null, null, null, testPaths},});
    }

    public TSPlanContextMetadataTest(String inputSQL, NamespaceType namespaceType, Path path,
            String dataType, String encoding, String[] encodingArgs, Path[] paths) {
        this.inputSQL = inputSQL;
        this.namespaceType = namespaceType;
        this.path = path;
        this.dataType = dataType;
        this.encoding = encoding;
        this.encodingArgs = encodingArgs;
        this.paths = paths;
    }

    @Test
    public void testanalyzeMetadata() throws QueryProcessorException, RecognitionException {
        TSqlParserV2 parser = new TSqlParserV2();
        MetadataOperator metadata = (MetadataOperator) parser.parseSQLToOperator(this.inputSQL);
        if (metadata == null)
            fail();
        MetadataPlan plan = (MetadataPlan) parser.transformToPhysicalPlan(metadata, null);
        assertEquals(namespaceType, plan.getNamespaceType());
        assertEquals(path, plan.getPath());
        assertEquals(dataType, plan.getDataType());
        assertEquals(encoding, plan.getEncoding());
        assertArrayEquals(encodingArgs, plan.getEncodingArgs());
        assertArrayEquals(paths, plan.getPaths().toArray());

    }

}
