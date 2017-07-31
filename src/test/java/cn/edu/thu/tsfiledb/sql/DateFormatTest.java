package cn.edu.thu.tsfiledb.sql;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;

public class DateFormatTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		String[] timeFormatWithoutMs = new String[]{
				"2016-02-01 11:12:35",
				"2016/02/01 11:12:35",
				"2016-02-01T11:12:35",
				"2016/02/01T11:12:35",
				"2016-02-01T11:12:35+08:00",
				"2016/02/01T11:12:35+08:00",
		};
		for(String t: timeFormatWithoutMs){
			DateTime d = DateTime.parse(t, DateTimeFormat.forPattern(SQLConstant.determineDateFormat(t)));
			assertEquals(d.toString(), "2016-02-01T11:12:35.000+08:00");
		}

		String[] timeFormatWithMs = new String[]{
				"2016/02/01 11:12:35.123",
				"2016-02-01 11:12:35.123",
				"2016-02-01T11:12:35.123",
				"2016/02/01T11:12:35.123",
				"2016-02-01T11:12:35.123+08:00",
				"2016/02/01T11:12:35.123+08:00",
		};
		for(String t: timeFormatWithMs){
			DateTime d = DateTime.parse(t, DateTimeFormat.forPattern(SQLConstant.determineDateFormat(t)));
			assertEquals(d.toString(), "2016-02-01T11:12:35.123+08:00");
		}
	}
	

	

}
