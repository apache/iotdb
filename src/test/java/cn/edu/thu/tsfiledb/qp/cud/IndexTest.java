package cn.edu.thu.tsfiledb.qp.cud;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.physical.crud.IndexPlan;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;

public class IndexTest {
	

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateIndex() throws QueryProcessorException, ArgsErrorException {
		
		String createIndex = "create index on root.laptop.d1.s1 using kv-match";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan =  (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(0, indexPlan.getParameters().keySet().size());
		assertEquals(0, indexPlan.getStartTime());
	}
	
	@Test
	public void testCreateIndex2() throws QueryProcessorException, ArgsErrorException{
		String createIndex = "create index on root.laptop.d1.s1 using kv-match with b=20,a=50 where time>=100";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(2, indexPlan.getParameters().keySet().size());
		Map<String, Integer> map = indexPlan.getParameters();
		assertEquals((long)20, (long)map.get("b"));
		assertEquals((long)50, (long)map.get("a"));
		assertEquals(100, indexPlan.getStartTime());
		createIndex = "create index on root.laptop.d1.s1 using kv-match with b=20,a=50 where time>100";
		processor = new QueryProcessor(new MemIntQpExecutor());
		indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals(101, indexPlan.getStartTime());
	}

}
