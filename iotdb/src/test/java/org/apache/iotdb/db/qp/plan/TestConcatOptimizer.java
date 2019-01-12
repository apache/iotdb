package org.apache.iotdb.db.qp.plan;

import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.common.Path;
import org.antlr.runtime.RecognitionException;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;


/**
 * test the correctness of
 * {@linkplain ConcatPathOptimizer ConcatPathOptimizer}
 */
public class TestConcatOptimizer {

  private static final Logger LOG = LoggerFactory.getLogger(TestConcatOptimizer.class);
  private QueryProcessor processor;

  @Before
  public void before() throws ProcessorException {
    MemIntQpExecutor memProcessor = new MemIntQpExecutor();
    Map<String, List<String>> fakeAllPaths = new HashMap<String, List<String>>() {{
      put("root.laptop.d1.s1", new ArrayList<String>() {{
        add("root.laptop.d1.s1");
      }});
      put("root.laptop.d1.s2", new ArrayList<String>() {{
        add("root.laptop.d1.s2");
      }});

      put("root.laptop.d2.s1", new ArrayList<String>() {{
        add("root.laptop.d2.s1");
      }});
      put("root.laptop.d2.s2", new ArrayList<String>() {{
        add("root.laptop.d2.s2");
      }});
      put("root.laptop.d3.s1", new ArrayList<String>() {{
        add("root.laptop.d3.s1");
      }});
      put("root.laptop.d3.s2", new ArrayList<String>() {{
        add("root.laptop.d3.s2");
      }});

      put("root.laptop.*.s1", new ArrayList<String>() {{
        add("root.laptop.d1.s1");
        add("root.laptop.d2.s1");
        add("root.laptop.d3.s1");
      }});
      put("root.laptop.*.s2", new ArrayList<String>() {{
        add("root.laptop.d1.s2");
        add("root.laptop.d2.s2");
        add("root.laptop.d3.s2");
      }});
    }};
    memProcessor.setFakeAllPaths(fakeAllPaths);
    processor = new QueryProcessor(memProcessor);
  }


  @Test
  public void testConcat1() throws QueryProcessorException, RecognitionException, ArgsErrorException, ProcessorException {
    String inputSQL = "select s1 from root.laptop.d1";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    assertEquals("root.laptop.d1.s1", plan.getPaths().get(0).toString());
  }

  @Test
  public void testConcat2() throws QueryProcessorException, RecognitionException, ArgsErrorException, ProcessorException {
    String inputSQL = "select s1 from root.laptop.*";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    assertEquals("root.laptop.d1.s1", plan.getPaths().get(0).toString());
    assertEquals("root.laptop.d2.s1", plan.getPaths().get(1).toString());
    assertEquals("root.laptop.d3.s1", plan.getPaths().get(2).toString());
  }

  @Test
  public void testConcat3() throws QueryProcessorException, RecognitionException, ArgsErrorException, ProcessorException {
    String inputSQL = "select s1 from root.laptop.d1 where s1 < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    SingleSeriesExpression seriesExpression = new SingleSeriesExpression(new Path("root.laptop.d1.s1"), ValueFilter.lt(10));
    assertEquals(seriesExpression.toString(), ((QueryPlan) plan).getExpression().toString());
  }

}