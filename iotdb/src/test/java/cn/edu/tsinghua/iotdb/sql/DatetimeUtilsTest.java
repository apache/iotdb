package cn.edu.tsinghua.iotdb.sql;

import cn.edu.tsinghua.iotdb.exception.qp.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.constant.DatetimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class DatetimeUtilsTest {
	private ZoneOffset zoneOffset;
	private ZoneId zoneId;

	@Before
	public void setUp() throws Exception {
		zoneOffset =  ZonedDateTime.now().getOffset();
		zoneId = ZoneId.systemDefault();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConvertDatetimeStrToLongWithoutMS() throws LogicalOperatorException {
		// 1546413207689
		// 2019-01-02T15:13:27.689+08:00
		String[] timeFormatWithoutMs = new String[]{
				"2019-01-02 15:13:27",
				"2019/01/02 15:13:27",
				"2019.01.02 15:13:27",
				"2019-01-02T15:13:27",
				"2019/01/02T15:13:27",
				"2019.01.02T15:13:27",
				"2019-01-02 15:13:27" + zoneOffset,
				"2019/01/02 15:13:27" + zoneOffset,
				"2019.01.02 15:13:27" + zoneOffset,
				"2019-01-02T15:13:27" + zoneOffset,
				"2019/01/02T15:13:27" + zoneOffset,
				"2019.01.02T15:13:27" + zoneOffset,
		};

		long res = 1546413207000L;
		for(String str : timeFormatWithoutMs) {
			assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneOffset));
		}
		
		for(String str : timeFormatWithoutMs) {
			assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneId));
		}
		
	}
	
	@Test
	public void testConvertDatetimeStrToLongWithMS() throws LogicalOperatorException {
		// 1546413207689
		// 2019-01-02T15:13:27.689+08:00
		String[] timeFormatWithoutMs = new String[]{
				"2019-01-02 15:13:27.689",
				"2019/01/02 15:13:27.689",
				"2019.01.02 15:13:27.689",
				"2019-01-02T15:13:27.689",
				"2019/01/02T15:13:27.689",
				"2019.01.02T15:13:27.689",
				"2019-01-02 15:13:27.689" + zoneOffset,
				"2019/01/02 15:13:27.689" + zoneOffset,
				"2019.01.02 15:13:27.689" + zoneOffset,
				"2019-01-02T15:13:27.689" + zoneOffset,
				"2019/01/02T15:13:27.689" + zoneOffset,
				"2019.01.02T15:13:27.689" + zoneOffset,
		};

		long res = 1546413207689L;
		for(String str : timeFormatWithoutMs) {
			assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneOffset));
		}
		
		for(String str : timeFormatWithoutMs) {
			assertEquals(res, DatetimeUtils.convertDatetimeStrToMillisecond(str, zoneId));
		}
	}
	
	
	public void createTest() {
//		long timestamp = System.currentTimeMillis();
//		System.out.println(timestamp);
//		ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("+08:00"));
//		System.out.println(zonedDateTime);
	}
	
	public static void main(String[] args) throws LogicalOperatorException {
//		ZoneId id = ZoneId.of("+08:00");
//		Instant instant = Instant.now(); 
//		ZoneOffset zoneOffset = id.getRules().getOffset(instant);
//		System.out.println(currentOffsetForMyZone);
		
//		DatetimeUtils.convertDatetimeStrToMillisecond("2019.01.02T15:13:27.689[Asia/Shanghai]", zoneOffset);
//		for(String string : ZoneId.getAvailableZoneIds()) {
//			ZoneId id = ZoneId.of(string);
//			Instant instant = Instant.now(); 
//			ZoneOffset currentOffsetForMyZone = id.getRules().getOffset(instant);
//			System.out.println(string+"--"+currentOffsetForMyZone);
//		}
//		long timestamp = System.currentTimeMillis();
//		System.out.println(timestamp);
//		LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(1546499858912L), ZoneId.systemDefault());
//		System.out.println(dateTime);
		System.out.println(DatetimeUtils.convertDatetimeStrToMillisecond("2018-1-1T13:00:07", ZoneId.systemDefault()));
	}
}


