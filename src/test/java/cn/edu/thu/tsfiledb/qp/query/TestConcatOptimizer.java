package cn.edu.thu.tsfiledb.qp.query;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;
import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.antlr.runtime.RecognitionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;


/**
 * test the correctness of {@linkplain cn.edu.thu.tsfiledb.qp.strategy.optimizer.ConcatPathOptimizer ConcatPathOptimizer}
 *
 * @author kangrong
 */
@RunWith(Parameterized.class)
public class TestConcatOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(TestConcatOptimizer.class);
    private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());

    private final String inputSQL;
    private final String retPlan;

    public TestConcatOptimizer(String inputSQL, String retPlan) {
        this.inputSQL = inputSQL;
        this.retPlan = retPlan;
    }

    @Before
    public void before() throws ProcessorException {
        Path path1 = new Path(new StringContainer(
                new String[]{"root", "laptop", "d1", "s1"},
                SystemConstant.PATH_SEPARATOR));
        Path path2 = new Path(new StringContainer(
                new String[]{"root", "laptop", "d1", "s2"},
                SystemConstant.PATH_SEPARATOR));
        Path path3 = new Path(new StringContainer(
                new String[]{"root", "laptop", "d2", "s1"},
                SystemConstant.PATH_SEPARATOR));
        Path path4 = new Path(new StringContainer(
                new String[]{"root", "laptop", "d2", "s2"},
                SystemConstant.PATH_SEPARATOR));
        processor.getExecutor().insert(path1, 1, Integer.toString(1));
        processor.getExecutor().insert(path2, 1, Integer.toString(1));
        processor.getExecutor().insert(path3, 1, Integer.toString(1));
        processor.getExecutor().insert(path4, 1, Integer.toString(1));
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays
                .asList(new Object[][]{
                        {
                                "select s1 from root.laptop.d1, root.laptop.d2 where s1 > 5",
                                "showing series plan:0\n" +
                                        "  series getIndex plan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d2.s1>5][root.laptop.d1.s1>5]]\n"
                        },
                        {
                                "select s1 from root.laptop.d1, root.laptop.d2 where root.laptop.d2.s2 > 10",
                                "showing series plan:0\n" +
                                        "  series getIndex plan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [root.laptop.d2.s2>10]\n"
                        },
                        {
                                "select s1 from root.laptop.d1, root.laptop.d2 where s1 > 5 and time > 5 or s2 < 10",
                                "showing series plan:0\n" +
                                        "  series getIndex plan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  [time>5]\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d2.s1>5][root.laptop.d1.s1>5]]\n" +
                                        "\n" +
                                        "showing series plan:1\n" +
                                        "  series getIndex plan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d2.s2<10][root.laptop.d1.s2<10]]\n"
                        },
                });
    }

    @Test
    public void testQueryBasic() throws QueryProcessorException, RecognitionException, ArgsErrorException {
        LOG.info("input SQL String:{}", inputSQL);
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
        assertEquals(retPlan, plan.printQueryPlan());
    }

}