package cn.edu.tsinghua.iotdb.qp.strategy;

import cn.edu.tsinghua.iotdb.exception.qp.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class LogicalGeneratorTest {
	LogicalGenerator generator;

	@Before
	public void setUp() throws Exception {
		generator = new LogicalGenerator(ZonedDateTime.now().getOffset());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseTimeFormatNow() throws LogicalOperatorException {
		long now = generator.parseTimeFormat(SQLConstant.NOW_FUNC);
		for(int i = 0; i <= 12;i++) {
			ZoneOffset offset1, offset2;
			if(i < 10) {
				offset1 = ZoneOffset.of("+0"+i+":00");
				offset2 = ZoneOffset.of("-0"+i+":00");
			} else {
				offset1 = ZoneOffset.of("+"+i+":00");
				offset2 = ZoneOffset.of("-"+i+":00");
			}
			ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of(offset1.toString()));
			assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
			zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of(offset2.toString()));
			assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
		}
		
	}
	
	@Test(expected = LogicalOperatorException.class)
	public void testParseTimeFormatFail1() throws LogicalOperatorException {
		generator.parseTimeFormat(null);
	}
	
	@Test(expected = LogicalOperatorException.class)
	public void testParseTimeFormatFail2() throws LogicalOperatorException {
		generator.parseTimeFormat("");
	}
}
