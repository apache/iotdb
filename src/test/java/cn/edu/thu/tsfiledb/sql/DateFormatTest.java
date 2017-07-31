package cn.edu.thu.tsfiledb.sql;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;

public class DateFormatTest {
	private static final String TIME_FORMAT_PATTERN = "^\\d{4}%s\\d{1,2}%s\\d{1,2}%s\\d{1,2}:\\d{2}:\\d{2}%s$";
	public static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {
		private static final long serialVersionUID = -1003441250297910762L;
		{
			put(String.format(TIME_FORMAT_PATTERN, "-", "-", "\\s", ""), "yyyy-MM-dd HH:mm:ss");
			put(String.format(TIME_FORMAT_PATTERN, "/", "/", "\\s", ""), "yyyy/MM/dd HH:mm:ss");
			put(String.format(TIME_FORMAT_PATTERN, "-", "-", "T", ""), "yyyy-MM-dd'T'HH:mm:ss");
			put(String.format(TIME_FORMAT_PATTERN, "/", "/", "T", ""), "yyyy/MM/dd'T'HH:mm:ss");
			put(String.format(TIME_FORMAT_PATTERN, "-", "-", "T", "(\\+|-)\\d{2}:\\d{2}"), "yyyy-MM-dd'T'HH:mm:ssZZ");
			put(String.format(TIME_FORMAT_PATTERN, "/", "/", "T", "(\\+|-)\\d{2}:\\d{2}"), "yyyy/MM/dd'T'HH:mm:ssZZ");
			put(String.format(TIME_FORMAT_PATTERN, "/", "/", "\\s", "\\.\\d{3}"), "yyyy/MM/dd HH:mm:ss.SSS");
			put(String.format(TIME_FORMAT_PATTERN, "-", "-", "\\s", "\\.\\d{3}"), "yyyy-MM-dd HH:mm:ss.SSS");
			put(String.format(TIME_FORMAT_PATTERN, "/", "/", "T", "\\.\\d{3}"), "yyyy/MM/dd'T'HH:mm:ss.SSS");
			put(String.format(TIME_FORMAT_PATTERN, "-", "-", "T", "\\.\\d{3}"), "yyyy-MM-dd'T'HH:mm:ss.SSS");
			put(String.format(TIME_FORMAT_PATTERN, "-", "-", "T", "\\.\\d{3}(\\+|-)\\d{2}:\\d{2}"), "yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
			put(String.format(TIME_FORMAT_PATTERN, "/", "/", "T", "\\.\\d{3}(\\+|-)\\d{2}:\\d{2}"), "yyyy/MM/dd'T'HH:mm:ss.SSSZZ");
		}
	};

	public static String determineDateFormat(String dateString) {
	    for (String regexp : SQLConstant.DATE_FORMAT_REGEXPS.keySet()) {
	        if (dateString.matches(regexp)) {
	            return SQLConstant.DATE_FORMAT_REGEXPS.get(regexp);
	        }
	    }
		return null;
	}
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
