package org.apache.iotdb.db.metrics.sink;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

public class ConsoleSink implements Sink {

	public MetricRegistry registry;
	public ConsoleReporter reporter;

	public ConsoleSink(MetricRegistry registry) {
		this.registry = registry;
		this.reporter = ConsoleReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS)
				.convertRatesTo(TimeUnit.SECONDS).build();
	}

	@Override
	public void start() {
		reporter.start(10, TimeUnit.SECONDS);
	}

	@Override
	public void stop() {
		reporter.stop();
	}

	@Override
	public void report() {
		reporter.report();
	}
}
