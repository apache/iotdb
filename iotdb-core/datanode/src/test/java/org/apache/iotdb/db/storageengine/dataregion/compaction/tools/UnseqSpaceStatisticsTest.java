package org.apache.iotdb.db.storageengine.dataregion.compaction.tools;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.Interval;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.UnseqSpaceStatistics;

import org.junit.Assert;
import org.junit.Test;

public class UnseqSpaceStatisticsTest {

  @Test
  public void test01() {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();
    unseqSpaceStatistics.update("root.db.d1", "s1", new Interval(1, 10));
    unseqSpaceStatistics.update("root.db.d1", "s1", new Interval(5, 15));
    unseqSpaceStatistics.update("root.db.d1", "s2", new Interval(1, 10));
    unseqSpaceStatistics.update("root.db.d2", "s2", new Interval(1, 10));

    Assert.assertEquals(2, unseqSpaceStatistics.getDeviceStatisticMap().size());
    Assert.assertEquals(2, unseqSpaceStatistics.getDeviceStatisticMap().get("root.db.d1").size());
    Assert.assertEquals(1, unseqSpaceStatistics.getDeviceStatisticMap().get("root.db.d2").size());
  }

  @Test
  public void test02() {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();
    unseqSpaceStatistics.update("root.db.d1", "s1", new Interval(1, 10));
    unseqSpaceStatistics.update("root.db.d1", "s1", new Interval(5, 15));
    unseqSpaceStatistics.update("root.db.d1", "s2", new Interval(1, 10));
    unseqSpaceStatistics.update("root.db.d2", "s2", new Interval(1, 10));

    Assert.assertTrue(unseqSpaceStatistics.hasOverlap("root.db.d1", "s1", new Interval(1, 10)));
    Assert.assertFalse(unseqSpaceStatistics.hasOverlap("root.db.d1", "s4", new Interval(1, 10)));
    Assert.assertFalse(unseqSpaceStatistics.hasOverlap("root.db.d2", "s1", new Interval(1, 10)));

    Assert.assertFalse(unseqSpaceStatistics.hasOverlap("root.db.d3", "s1", new Interval(1, 10)));
    Assert.assertFalse(unseqSpaceStatistics.hasOverlap("root.db.d1", "s1", new Interval(21, 30)));
  }
}
