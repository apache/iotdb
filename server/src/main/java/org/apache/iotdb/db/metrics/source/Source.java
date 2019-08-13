package org.apache.iotdb.db.metrics.source;

import com.codahale.metrics.MetricRegistry;

public interface Source {
	
	public String sourceName();
	
	public MetricRegistry metricRegistry();
	
}
