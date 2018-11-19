package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryConfig;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TimePlainEncodeReadTest {

	private static String fileName = "src/test/resources/perTestOutputData.ksn";
	private static TsRandomAccessLocalFileReader inputFile;
	private static QueryEngine engine = null;
	static ITsRandomAccessFileReader raf;
	private static QueryConfig configOneSeriesWithNoFilter = new QueryConfig("d1.s1");

	private static QueryConfig configTwoSeriesWithNoFilter = new QueryConfig("d1.s1|d2.s2");

	private static QueryConfig configWithTwoSeriesTimeValueNoCrossFilter = new QueryConfig("d2.s1|d2.s4",
			"0,(>=1480562618970)&(<1480562618977)", "null", "2,d2.s2,(>9722)");
	private static QueryConfig configWithTwoSeriesTimeValueNoCrossFilter2 = new QueryConfig("d2.s2",
			"0,(>=1480562618970)&(<1480562618977)", "null", "2,d2.s2,(!=9722)");

	private static QueryConfig configWithTwoSeriesTimeValueCrossFilter = new QueryConfig("d1.s1|d2.s2",
			"0,(>=1480562618970)&(<1480562618977)", "null", "[2,d2.s2,(>9722)]");

	private static QueryConfig configWithCrossSeriesTimeValueFilter = new QueryConfig("d1.s1|d2.s2",
			"0,(>=1480562618950)&(<=1480562618960)", "null", "[2,d2.s3,(>9541)|(<=9511)]&[2,d1.s1,(<=9562)]");

	private static QueryConfig configWithCrossSeriesTimeValueFilterOrOpe = new QueryConfig("d1.s1|d2.s2",
			"0,((>=1480562618906)&(<=1480562618915))|((>=1480562618928)&(<=1480562618933))", "null",
			"[2,d1.s1,(<=9321)]|[2,d2.s2,(>9312)]");

	private static QueryConfig booleanConfig = new QueryConfig("d1.s5",
			"0,(>=1480562618970)&(<=1480562618981)", "null", "2,d1.s5,(=false)");

	private static QueryConfig greatStringConfig = new QueryConfig("d1.s4",
			"0,(>=1480562618970)&(<=1480562618981)", "null", "2,d1.s4,(>dog97)");

	private static QueryConfig lessStringConfig = new QueryConfig("d1.s4",
			"0,(>=1480562618970)&(<=1480562618981)", "null", "2,d1.s4,(<dog97)");

	private static QueryConfig floatConfig = new QueryConfig("d1.s6",
			"0,(>=1480562618970)&(<=1480562618981)", "null", "2,d1.s6,(>103.0)");

	private static QueryConfig doubleConfig = new QueryConfig("d1.s7",
			"0,(>=1480562618021)&(<=1480562618033)", "null", "2,d1.s7,(<=7.0)");

	private static QueryConfig floatDoubleConfigFilter = new QueryConfig("d1.s6",
			"0,(>=1480562618005)&(<1480562618977)", "null", "2,d1.s6,(>=103.0)");

	@Before
	public void prepare() throws IOException, InterruptedException, WriteProcessException {
		TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "PLAIN";
		ReadPerf.generateFile();
	}

	@After
	public void after() {
		ReadPerf.after();
	}

	@Test
	public void queryOneMeasurementWithoutFilterTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(configOneSeriesWithNoFilter, fileName);

		int count = 0;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (count == 0) {
				assertEquals(r.timestamp, 1480562618010L);
			}
			if (count == 499) {
				assertEquals(r.timestamp, 1480562618999L);
			}
			count++;
		}
		assertEquals(count, 500);
	}

	@Test
	public void queryTwoMeasurementsWithoutFilterTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(configTwoSeriesWithNoFilter, fileName);
		int count = 0;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (count == 0) {
				if (count == 0) {
					assertEquals(1480562618005L, r.timestamp);
				}
			}
			count++;
		}
		assertEquals(count, 750);
//		// verify d1.s1
//		DynamicOneColumnData d1s1Data = onePassQueryDataSet.mapRet.get("d1.s1");
//		assertEquals(d1s1Data.length, 500);
//		assertEquals(d1s1Data.getTime(0), 1480562618010L);
//		assertEquals(d1s1Data.getInt(0), 101);
//		assertEquals(d1s1Data.getTime(d1s1Data.length - 1), 1480562618999L);
//		assertEquals(d1s1Data.getInt(d1s1Data.length - 1), 9991);
//
//		// verify d2.s2
//		DynamicOneColumnData d2s2Data = onePassQueryDataSet.mapRet.get("d2.s2");
//		assertEquals(d2s2Data.length, 750);
//		assertEquals(d2s2Data.getTime(500), 1480562618670L);
//		assertEquals(d2s2Data.getLong(500), 6702L);
//		assertEquals(d2s2Data.getTime(d2s2Data.length - 1), 1480562618999L);
//		assertEquals(d2s2Data.getLong(d2s2Data.length - 1), 9992L);
	}

	@Test
	public void queryTwoMeasurementsWithSingleFilterTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(configWithTwoSeriesTimeValueNoCrossFilter2, fileName);

		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			System.out.println(r);
		}

	}

	@Test
	public void queryWithTwoSeriesTimeValueFilterCrossTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(configWithTwoSeriesTimeValueCrossFilter, fileName);

		// time filter & value filter
		// verify d1.s1, d2.s1
		int cnt = 1;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 1) {
				assertEquals(r.timestamp, 1480562618973L);
			} else if (cnt == 2) {
				assertEquals(r.timestamp, 1480562618974L);
			} else if (cnt == 3) {
				assertEquals(r.timestamp, 1480562618975L);
			}
			//System.out.println(r);
			cnt++;
		}
		assertEquals(cnt, 5);
	}

	@Test
	public void queryWithCrossSeriesTimeValueFilterTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(configWithCrossSeriesTimeValueFilter, fileName);
		// time filter & value filter
		// verify d1.s1, d2.s1
		/**
		 1480562618950	9501	9502
		 1480562618954	9541	9542
		 1480562618955	9551	9552
		 1480562618956	9561	9562
		 */
		int cnt = 1;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 1) {
				assertEquals(r.timestamp, 1480562618950L);
			} else if (cnt == 2) {
				assertEquals(r.timestamp, 1480562618954L);
			} else if (cnt == 3) {
				assertEquals(r.timestamp, 1480562618955L);
			} else if (cnt == 4) {
				assertEquals(r.timestamp, 1480562618956L);
			}
			//System.out.println(r);
			cnt++;
		}
		assertEquals(cnt, 5);

		OnePassQueryDataSet onePassQueryDataSetOrOpe = QueryEngine.query(configWithCrossSeriesTimeValueFilterOrOpe, fileName);
		// time filter & value filter
		// verify d1.s1, d2.s1
		/**
		 1480562618910	9101	9102
		 1480562618911	9111	9112
		 1480562618912	9121	9122
		 1480562618913	9131	9132
		 1480562618914	9141	9142
		 1480562618915	9151	9152
		 1480562618930	9301	9302
		 1480562618931	9311	9312
		 1480562618932	9321	9322
		 1480562618933	9331	9332
		 */
		cnt = 1;
		while (onePassQueryDataSetOrOpe.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSetOrOpe.getNextRecord();
			//System.out.println(r);
			if (cnt == 4) {
				assertEquals(r.timestamp, 1480562618913L);
			} else if (cnt == 7) {
				assertEquals(r.timestamp, 1480562618930L);
			}
			cnt++;
		}
		assertEquals(cnt, 11);
	}

	// @Test
	public void queryBooleanTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(booleanConfig, fileName);
		int cnt = 1;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 1) {
				assertEquals(r.getTime(), 1480562618972L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getBoolV(), false);
			}
			if (cnt == 2) {
				assertEquals(r.getTime(), 1480562618981L);
				Field f2 = r.getFields().get(0);
				assertEquals(f2.getBoolV(), false);
			}
			cnt++;
		}
	}

	@Test
	public void queryStringTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(lessStringConfig, fileName);
		int cnt = 0;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 1) {
				assertEquals(r.getTime(), 1480562618976L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getStringValue(), "dog976");
			}
			// System.out.println(r);
			cnt++;
		}
		Assert.assertEquals(cnt, 0);

		onePassQueryDataSet = QueryEngine.query(greatStringConfig, fileName);
		cnt = 0;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 0) {
				assertEquals(r.getTime(), 1480562618976L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getStringValue(), "dog976");
			}
			// System.out.println(r);
			cnt++;
		}
		Assert.assertEquals(cnt, 1);
	}

	@Test
	public void queryFloatTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(floatConfig, fileName);
		int cnt = 0;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 1) {
				assertEquals(r.getTime(), 1480562618980L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getFloatV(), 108.0, 0.0);
			}
			if (cnt == 2) {
				assertEquals(r.getTime(), 1480562618990L);
				Field f2 = r.getFields().get(0);
				assertEquals(f2.getFloatV(), 110.0, 0.0);
			}
			cnt++;
		}
	}

	@Test
	public void queryDoubleTest() throws IOException {
		OnePassQueryDataSet onePassQueryDataSet = QueryEngine.query(doubleConfig, fileName);
		int cnt = 1;
		while (onePassQueryDataSet.hasNextRecord()) {
			OldRowRecord r = onePassQueryDataSet.getNextRecord();
			if (cnt == 1) {
				assertEquals(r.getTime(), 1480562618022L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getDoubleV(), 2.0, 0.0);
			}
			if (cnt == 2) {
				assertEquals(r.getTime(), 1480562618033L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getDoubleV(), 3.0, 0.0);
			}
			cnt++;
		}
	}
}
