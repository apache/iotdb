package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import static org.junit.Assert.*;
import org.junit.Test;

public class LongStatisticsTest {

	@Test
	public void testUpdate() {
		Statistics<Long> longStats = new LongStatistics();
		assertEquals(true, longStats.isEmpty());
		long firstValue = -120985402913209l;
		long secondValue = 1251465332132513l;
		longStats.updateStats(firstValue);
		assertEquals(false, longStats.isEmpty());
		longStats.updateStats(secondValue);
		assertEquals(false, longStats.isEmpty());
		assertEquals(secondValue, (long) longStats.getMax());
		assertEquals(firstValue, (long) longStats.getMin());
		assertEquals(firstValue, (long) longStats.getFirst());
		assertEquals(firstValue + secondValue, (long) longStats.getSum());
		assertEquals(secondValue, (long) longStats.getLast());
	}

	@Test
	public void testMerge() {
		Statistics<Long> longStats1 = new LongStatistics();
		Statistics<Long> longStats2 = new LongStatistics();
		assertEquals(true, longStats1.isEmpty());
		assertEquals(true, longStats2.isEmpty());
		long max1 = 100000000000l;
		long max2 = 200000000000l;
		longStats1.updateStats(1l);
		longStats1.updateStats(max1);
		longStats2.updateStats(max2);

		Statistics<Long> longStats3 = new LongStatistics();
		longStats3.mergeStatistics(longStats1);
		assertEquals(false, longStats3.isEmpty());
		assertEquals(max1, (long) longStats3.getMax());
		assertEquals(1, (long) longStats3.getMin());
		assertEquals(max1 + 1, (long) longStats3.getSum());
		assertEquals(1, (long) longStats3.getFirst());
		assertEquals(max1, (long) longStats3.getLast());

		longStats3.mergeStatistics(longStats2);
		assertEquals(max2, (long) longStats3.getMax());
		assertEquals(1, (long) longStats3.getMin());
		assertEquals(max2 + max1 + 1, (long) longStats3.getSum());
		assertEquals(1, (long) longStats3.getFirst());
		assertEquals(max2, (long) longStats3.getLast());

		// Test mismatch
		IntegerStatistics intStats5 = new IntegerStatistics();
		intStats5.updateStats(-10000);
		try {
			longStats3.mergeStatistics(intStats5);
		} catch (StatisticsClassException e) {
			// that's true route
		} catch (Exception e) {
			fail();
		}

		assertEquals(max2, (long) longStats3.getMax());
		// if not merge, the min value will not be changed by smaller value in
		// intStats5
		assertEquals(1, (long) longStats3.getMin());
		assertEquals(max2 + max1 + 1, (long) longStats3.getSum());
		assertEquals(1, (long) longStats3.getFirst());
		assertEquals(max2, (long) longStats3.getLast());
	}

}
