package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.utils.FileGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimePlainEncodeReadTest {

	private static String fileName = "target/perTestOutputData.tsfile";
	private static ReadOnlyTsFile roTsFile = null;

	@Before
	public void prepare() throws IOException, InterruptedException, WriteProcessException {
		TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "PLAIN";
		FileGenerator.generateFile();
		TsFileSequenceReader reader = new TsFileSequenceReader(fileName);
		roTsFile = new ReadOnlyTsFile(reader);
	}

	@After
	public void after() throws IOException {
		if (roTsFile != null)
			roTsFile.close();
		FileGenerator.after();
	}

	@Test
	public void queryOneMeasurementWithoutFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s1"));
		QueryExpression queryExpression = QueryExpression.create(pathList, null);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int count = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (count == 0) {
				assertEquals(r.getTimestamp(), 1480562618010L);
			}
			if (count == 499) {
				assertEquals(r.getTimestamp(), 1480562618999L);
			}
			count++;
		}
		assertEquals(count, 500);
	}

	@Test
	public void queryTwoMeasurementsWithoutFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s1"));
		pathList.add(new Path("d2.s2"));
		QueryExpression queryExpression = QueryExpression.create(pathList, null);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int count = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (count == 0) {
				if (count == 0) {
					assertEquals(1480562618005L, r.getTimestamp());
				}
			}
			count++;
		}
		assertEquals(count, 750);
	}

	@Test
	public void queryTwoMeasurementsWithSingleFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d2.s1"));
		pathList.add(new Path("d2.s4"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d2.s2"), ValueFilter.gt(9722L));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.lt(1480562618977L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		while (dataSet.hasNext()) {
			dataSet.next();
		}

	}

	@Test
	public void queryWithTwoSeriesTimeValueFilterCrossTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d2.s2"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d2.s2"), ValueFilter.notEq(9722L));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.lt(1480562618977L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		// time filter & value filter
		// verify d1.s1, d2.s1
		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618970L);
			} else if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618971L);
			} else if (cnt == 3) {
				assertEquals(r.getTimestamp(), 1480562618973L);
			}
			//System.out.println(r);
			cnt++;
		}
		assertEquals(cnt, 7);
	}

	@Test
	public void queryWithCrossSeriesTimeValueFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s1"));
		pathList.add(new Path("d2.s2"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d2.s2"), ValueFilter.gt(9722L));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.lt(1480562618977L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		// time filter & value filter
		// verify d1.s1, d2.s1
		/**
		 1480562618950	9501	9502
		 1480562618954	9541	9542
		 1480562618955	9551	9552
		 1480562618956	9561	9562
		 */
		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618973L);
			} else if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618974L);
			} else if (cnt == 3) {
				assertEquals(r.getTimestamp(), 1480562618975L);
			} else if (cnt == 4) {
				assertEquals(r.getTimestamp(), 1480562618976L);
			}
			//System.out.println(r);
			cnt++;
		}
		assertEquals(cnt, 5);

		pathList.clear();
		pathList.add(new Path("d1.s1"));
		pathList.add(new Path("d2.s2"));
		valFilter = new SingleSeriesExpression(new Path("d1.s1"), ValueFilter.ltEq(9321));
		valFilter = BinaryExpression.and(new SingleSeriesExpression(new Path("d2.s2"), ValueFilter.ltEq(9312L)),
				valFilter);
		tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618906L)),
				new GlobalTimeExpression(TimeFilter.ltEq(1480562618915L)));
		tFilter = BinaryExpression.or(tFilter,
				BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618928L)),
						new GlobalTimeExpression(TimeFilter.ltEq(1480562618933L))));
		finalFilter = BinaryExpression.and(valFilter, tFilter);
		queryExpression = QueryExpression.create(pathList, finalFilter);
		dataSet = roTsFile.query(queryExpression);

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
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			//System.out.println(r);
			if (cnt == 4) {
				assertEquals(r.getTimestamp(), 1480562618913L);
			} else if (cnt == 7) {
				assertEquals(r.getTimestamp(), 1480562618930L);
			}
			cnt++;
		}
		assertEquals(cnt, 9);
	}

	@Test
	public void queryBooleanTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s5"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d1.s5"), ValueFilter.eq(false));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.lt(1480562618981L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			System.out.println(r);
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618972L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getBoolV(), false);
			}
			if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618981L);
				Field f2 = r.getFields().get(0);
				assertEquals(f2.getBoolV(), false);
			}
			cnt++;
		}
	}

	@Test
	public void queryStringTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s4"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d1.s4"), ValueFilter.gt(new Binary("dog97")));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.ltEq(1480562618981L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 0) {
				assertEquals(r.getTimestamp(), 1480562618976L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.toString(), "dog976");
			}
			// System.out.println(r);
			cnt++;
		}
		Assert.assertEquals(cnt, 1);

		pathList = new ArrayList<>();
		pathList.add(new Path("d1.s4"));
		valFilter = new SingleSeriesExpression(new Path("d1.s4"), ValueFilter.lt(new Binary("dog97")));
		tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.ltEq(1480562618981L)));
		finalFilter = BinaryExpression.and(valFilter, tFilter);
		queryExpression = QueryExpression.create(pathList, finalFilter);
		dataSet = roTsFile.query(queryExpression);
		cnt = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618976L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getBinaryV(), "dog976");
			}
			// System.out.println(r);
			cnt++;
		}
		Assert.assertEquals(cnt, 0);

	}

	@Test
	public void queryFloatTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s6"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d1.s6"), ValueFilter.gt(103.0f));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeExpression(TimeFilter.ltEq(1480562618981L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618980L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getFloatV(), 108.0, 0.0);
			}
			if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618990L);
				Field f2 = r.getFields().get(0);
				assertEquals(f2.getFloatV(), 110.0, 0.0);
			}
			cnt++;
		}
	}

	@Test
	public void queryDoubleTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s7"));
		IExpression valFilter = new SingleSeriesExpression(new Path("d1.s7"), ValueFilter.gt(7.0));
		IExpression tFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1480562618021L)),
				new GlobalTimeExpression(TimeFilter.ltEq(1480562618033L)));
		IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618022L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getDoubleV(), 2.0, 0.0);
			}
			if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618033L);
				Field f1 = r.getFields().get(0);
				assertEquals(f1.getDoubleV(), 3.0, 0.0);
			}
			cnt++;
		}
	}
}
