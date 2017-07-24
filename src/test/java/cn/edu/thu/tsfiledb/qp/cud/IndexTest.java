package cn.edu.thu.tsfiledb.qp.cud;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;

public class IndexTest {
	
	private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateIndex() throws QueryProcessorException, ArgsErrorException {
		
		String createIndex = "create index on root.laptop.d1.s1 using kv-match";
		String sql = "delete from root.laptop.d1.s1 where time<100";
		processor.parseSQLToPhysicalPlan(createIndex);
		
	}
	
	@Test
	public void testCreateIndex2() throws QueryProcessorException, ArgsErrorException{
		String sql = "create timeseries root.laptop.d1.s1 with datatype=INT32,encoding=RLE";
		String createIndex = "create index on root.laptop.d1.s1 using kv-match with window_length=20,a=50 where time>=100";
		processor.parseSQLToPhysicalPlan(createIndex);
	}

}
