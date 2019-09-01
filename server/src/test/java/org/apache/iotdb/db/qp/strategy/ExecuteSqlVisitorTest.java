package org.apache.iotdb.db.qp.strategy;

import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class ExecuteSqlVisitorTest {

  ExecuteSqlVisitor visitor;

  @Before
  public void setUp() throws Exception {
    visitor = new ExecuteSqlVisitor(ZonedDateTime.now().getOffset());
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testParseTimeFormatNow() throws LogicalOperatorException {
    long now = visitor.parseTimeFormat(SQLConstant.NOW_FUNC);
    for (int i = 0; i <= 12; i++) {
      ZoneOffset offset1, offset2;
      if (i < 10) {
        offset1 = ZoneOffset.of("+0" + i + ":00");
        offset2 = ZoneOffset.of("-0" + i + ":00");
      } else {
        offset1 = ZoneOffset.of("+" + i + ":00");
        offset2 = ZoneOffset.of("-" + i + ":00");
      }
      ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now),
              ZoneId.of(offset1.toString()));
      assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
      zonedDateTime = ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(now), ZoneId.of(offset2.toString()));
      assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
    }
  }

  @Test(expected = SqlParseException.class)
  public void testParseTimeFormatFail1() throws LogicalOperatorException {
    visitor.parseTimeFormat(null);
  }

  @Test(expected = SqlParseException.class)
  public void testParseTimeFormatFail2() throws LogicalOperatorException {
    visitor.parseTimeFormat("");
  }
}
