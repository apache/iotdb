package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.junit.Test;

public class BigDecimalStatisticsTest {
	private static final double maxError = 0.0001d;

	@Test
	public void testUpdate() {
		Statistics<BigDecimal> bigStats = new BigDecimalStatistics();
		BigDecimal up1 = new BigDecimal("1.232");
		BigDecimal up2 = new BigDecimal("2.232");
		bigStats.updateStats(up1);
		assertEquals(false, bigStats.isEmpty());
		bigStats.updateStats(up2);
		assertEquals(false, bigStats.isEmpty());
		assertEquals(up2, (BigDecimal) bigStats.getMax());
		assertEquals(up1, (BigDecimal) bigStats.getMin());
		assertEquals(up2.add(up1).doubleValue(), bigStats.getSum(),maxError);
		assertEquals(up1, (BigDecimal) bigStats.getFirst());
		assertEquals(up2, (BigDecimal) bigStats.getLast());
	}

	@Test
	public void testMerge() {
		Statistics<BigDecimal> bigStats1 = new BigDecimalStatistics();
		Statistics<BigDecimal> bigStats2 = new BigDecimalStatistics();

		BigDecimal down1 = new BigDecimal("1.232");
		BigDecimal up1 = new BigDecimal("2.232");
		bigStats1.updateStats(down1);
		bigStats1.updateStats(up1);
		BigDecimal up2 = new BigDecimal("200.232");
		bigStats2.updateStats(up2);

		Statistics<BigDecimal> bigStats3 = new BigDecimalStatistics();
		bigStats3.mergeStatistics(bigStats1);
		assertEquals(false, bigStats3.isEmpty());
		assertEquals(up1, (BigDecimal) bigStats3.getMax());
		assertEquals(down1, (BigDecimal) bigStats3.getMin());
		assertEquals(up1.add(down1).doubleValue(), bigStats3.getSum(),maxError);
		assertEquals(down1, (BigDecimal) bigStats3.getFirst());
		assertEquals(up1, (BigDecimal) bigStats3.getLast());

		bigStats3.mergeStatistics(bigStats2);
		assertEquals(up2, (BigDecimal) bigStats3.getMax());
		assertEquals(down1, (BigDecimal) bigStats3.getMin());
		assertEquals(up1.add(down1).add(up2).doubleValue(), bigStats3.getSum(),maxError);
		assertEquals(down1, (BigDecimal) bigStats3.getFirst());
		assertEquals(up2, (BigDecimal) bigStats3.getLast());

	}

}
