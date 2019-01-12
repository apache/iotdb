package org.apache.iotdb.tsfile.file.metadata.statistics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IntegerStatisticsTest {

  @Test
  public void testUpdate() {
    Statistics<Integer> intStats = new IntegerStatistics();
    intStats.updateStats(1);
    assertEquals(false, intStats.isEmpty());
    intStats.updateStats(2);
    assertEquals(false, intStats.isEmpty());
    assertEquals(2, (int) intStats.getMax());
    assertEquals(1, (int) intStats.getMin());
    assertEquals(1, (int)intStats.getFirst());
    assertEquals(3, (int)intStats.getSum());
    assertEquals(2, (int)intStats.getLast());
  }

  @Test
  public void testMerge() {
    Statistics<Integer> intStats1 = new IntegerStatistics();
    Statistics<Integer> intStats2 = new IntegerStatistics();

    intStats1.updateStats(1);
    intStats1.updateStats(100);

    intStats2.updateStats(200);

    Statistics<Integer> intStats3 = new IntegerStatistics();
    intStats3.mergeStatistics(intStats1);
    assertEquals(false, intStats3.isEmpty());
    assertEquals(100, (int) intStats3.getMax());
    assertEquals(1, (int) intStats3.getMin());
    assertEquals(1, (int)intStats3.getFirst());
    assertEquals(1+100, (int)intStats3.getSum());
    assertEquals(100, (int)intStats3.getLast());
    
    intStats3.mergeStatistics(intStats2);
    assertEquals(200, (int) intStats3.getMax());
    assertEquals(1, (int) intStats3.getMin());
    assertEquals(1, (int)intStats3.getFirst());
    assertEquals(101+200, (int)intStats3.getSum());
    assertEquals(200, (int)intStats3.getLast());
  }
}
