package org.apache.iotdb.db.metrics.source;

import org.apache.iotdb.db.metrics.server.ServerArgument;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class MetricsSource implements Source {
	
	public ServerArgument serverArgument;
	public String sourceName = "iot-metrics";
	public MetricRegistry metricRegistry = new MetricRegistry();

	public MetricsSource(ServerArgument serverArgument) {
		this.serverArgument = serverArgument;

		metricRegistry.register(MetricRegistry.name("host"), new Gauge<String>() {
			public String getValue() {
				return serverArgument.getHost();
			}
		});

		metricRegistry.register(MetricRegistry.name("port"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getPort();
			}
		});

		metricRegistry.register(MetricRegistry.name("cores"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getCores();
			}
		});

		metricRegistry.register(MetricRegistry.name("cpu_ratio"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getCpuRatio();
			}
		});

		metricRegistry.register(MetricRegistry.name("total_memory"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getTotalMemory();
			}
		});

		metricRegistry.register(MetricRegistry.name("max_memory"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getMaxMemory();
			}
		});

		metricRegistry.register(MetricRegistry.name("free_memory"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getFreeMemory();
			}
		});

		metricRegistry.register(MetricRegistry.name("totalPhysical_memory"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getTotalPhysicalMemory();
			}
		});

		metricRegistry.register(MetricRegistry.name("freePhysical_memory"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getFreePhysicalMemory();
			}
		});

		metricRegistry.register(MetricRegistry.name("usedPhysical_memory"), new Gauge<Integer>() {
			public Integer getValue() {
				return (int) serverArgument.getUsedPhysicalMemory();
			}
		});
	}

	@Override
	public String sourceName() {
		return this.sourceName;
	}

	@Override
	public MetricRegistry metricRegistry() {
		return this.metricRegistry;
	}

	public ServerArgument getServerArgument() {
		return serverArgument;
	}

	public void setServerArgument(ServerArgument serverArgument) {
		this.serverArgument = serverArgument;
	}

}
