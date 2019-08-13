package org.apache.iotdb.db.metrics.source;

import java.lang.management.ManagementFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

public class JvmSource implements Source{
	
	public String sourceName = "jvm";
	public MetricRegistry metricRegistry = new MetricRegistry();

	public JvmSource() {
		metricRegistry.registerAll(new GarbageCollectorMetricSet());
		metricRegistry.registerAll(new MemoryUsageGaugeSet());
		metricRegistry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
	}

	@Override
	public String sourceName() {
		return this.sourceName;
	}

	@Override
	public MetricRegistry metricRegistry() {
		return this.metricRegistry;
	}
	
}
