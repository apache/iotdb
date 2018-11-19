package cn.edu.tsinghua.iotdb.qp.plan;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexPlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;

public class IndexTest {
	

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateIndex() throws QueryProcessorException, ArgsErrorException, ProcessorException {
		
		String createIndex = "create index on root.laptop.d1.s1 using kvindex";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan =  (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(1, indexPlan.getParameters().keySet().size());
//		assertEquals(0, indexPlan.getStartTime());
	}
	
	@Test
	public void testCreateIndex2() throws QueryProcessorException, ArgsErrorException, ProcessorException {
		String createIndex = "create index on root.laptop.d1.s1 using kvindex with b=20,a=50 where time>=100";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(3, indexPlan.getParameters().keySet().size());
		Map<String, Object> map = indexPlan.getParameters();
		assertEquals(20, map.get("b"));
		assertEquals(50, map.get("a"));
//		assertEquals(100, indexPlan.getStartTime());
		createIndex = "create index on root.laptop.d1.s1 using kvindex with b=20,a=50 where time>100";
		processor = new QueryProcessor(new MemIntQpExecutor());
		indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
//		assertEquals(101, indexPlan.getStartTime());
	}

}
