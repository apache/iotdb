package org.apache.iotdb.metrics.micrometer.registry;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.step.StepCounter;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import io.micrometer.core.instrument.util.NamedThreadFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class IoTDBMeterRegistry extends StepMeterRegistry {

    public IoTDBMeterRegistry(StepRegistryConfig config, Clock clock) {
        super(config, clock);
        start(new NamedThreadFactory("iotdb-metrics-publisher"));
    }

    @Override
    protected void publish() {
        getMeters().forEach(meter -> {
            Meter.Id id = meter.getId();
            String name = id.getName();
            List<Tag> tags = id.getTags();
            final double[] value = new double[1];
            meter.use(
                    gauge -> {
                        value[0] = gauge.value();
                    },
                    counter -> {
                        value[0] = counter.count();
                    }
                    ,
                    timer -> {
                        HistogramSnapshot snapshot = timer.takeSnapshot();
                        snapshot.to
                        value[0] = snapshot.count();
                    },
                    summary -> {
                        HistogramSnapshot snapshot = summary.takeSnapshot();
                        value[0] = snapshot.count();
                    },
                    longTaskTimer -> {
                        Meter.Id id = longTaskTimer.getId();
                    },
                    timeGauge -> {
                        Meter.Id id = timeGauge.getId();
                    },
                    functionCounter ->  {
                        Meter.Id id = functionCounter.getId();
                    },
                    timer -> {
                        Meter.Id id = timer.getId();
                    },
                    m -> {
                        Meter.Id id = m.getId();
                    }

            );
        });
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
