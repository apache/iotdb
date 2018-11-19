package cn.edu.tsinghua.tsfile.timeseries.read;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryConfig;
import org.junit.*;


public class QueryEngineTest {
	private QueryEngine engine;
	ITsRandomAccessFileReader raf;

	@BeforeClass
	public static void setUpBeforeClass() throws InterruptedException, WriteProcessException, IOException {
		QueryEnginePerf.generateFile();
	}

	@Before
	public void prepare() throws IOException, InterruptedException, WriteProcessException {
		String fileName = "src/test/resources/perTestOutputData.ksn";
		raf = new TsRandomAccessLocalFileReader(fileName);
		engine = new QueryEngine(raf, 10);


		List<Path> paths = new ArrayList<Path>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
	}

	@After
	public void after() throws IOException {
		raf.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws InterruptedException, WriteProcessException, IOException {
		QueryEnginePerf.after();
	}

	@Test
	public void readAllInOneColumn() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1|root.vehicle.d1.s2|root.vehicle.d1.s3");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(199, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	@Test
	public void readOneColumnWithTimeFilter() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1|root.vehicle.d1.s2", "0,(>178)&(<=198)", "null", "null");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(20, count);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void readOneColumnWithValueFilter() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1", "null", "null", "2,root.vehicle.d1.s1,(>=18901)");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(4, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	@Test
	public void readOneColumnWithTimeAndValueFilter1() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1", "0,(>278)&(<=298)", "null", "2,root.vehicle.d1.s1,(>10294)");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(0, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	@Test
	public void readOneColumnWithTimeAndValueFilter2() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1", "0,(>=186)", "null", "2,root.vehicle.d1.s1,(>10211)");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(6, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	@Test
	public void crossRead1() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1|root.vehicle.d1.s2", "0,(<=197)", "null", "[2,root.vehicle.d1.s1,(<10)]");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(8, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	@Test
	public void crossRead2() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1|root.vehicle.d1.s2", "0,(>=0)", "null", "[2,root.vehicle.d1.s2,(>17802)]");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(21, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	@Test
	public void crossRead3() {
		QueryConfig config = new QueryConfig("root.vehicle.d1.s1|root.vehicle.d1.s2|root.vehicle.d1.s3"
				, "0,(<=190)", "null", "[2,root.vehicle.d1.s2,(>17802)]&[2,root.vehicle.d1.s3,(>18703)&(<18903)]");
		try {
			OnePassQueryDataSet res = engine.query(config);
			int count = output(res, true);
			assertEquals(1, count);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

	private static int output(OnePassQueryDataSet res, boolean printToConsole) {
		int cnt = 0;

		//Output Labels
		if (printToConsole) {
			System.out.printf("+---------------+");
			for (int i = 0; i < res.mapRet.keySet().size(); i++) {
				System.out.printf("---------------+");
			}
			System.out.printf("\n");

			System.out.printf("|%15s|", "Timestamp");
			for (String name : res.mapRet.keySet()) {
				System.out.printf("%15s|", name);
			}
			System.out.printf("\n");

			System.out.printf("+---------------+");
			for (int i = 0; i < res.mapRet.keySet().size(); i++) {
				System.out.printf("---------------+");
			}
			System.out.printf("\n");
		}
		// output values
		OldRowRecord r;

		while ((r = res.getNextRecord()) != null) {
			StringBuilder line = new StringBuilder();
			line.append(String.valueOf(r.timestamp));

			if (printToConsole) {
				System.out.printf("|%15s|", String.valueOf(r.timestamp));
			}

			for (int i = 0; i < r.fields.size(); i++) {
				line.append("\t" + r.fields.get(i).getStringValue());
				if (printToConsole) {
					System.out.printf("%15s|", String.valueOf(r.fields.get(i).getStringValue()));
				}
			}

			if (printToConsole) {
				System.out.printf("\n");
			}

//			bw.write(line.toString());
//			bw.newLine();
			cnt++;
		}

		if (printToConsole) {
			System.out.printf("+---------------+");
			for (int i = 0; i < res.mapRet.keySet().size(); i++) {
				System.out.printf("---------------+");
			}
			System.out.printf("\n");
		}
//		bw.close();
		System.out.println("Result size : " + cnt);
		return cnt;
	}
}
