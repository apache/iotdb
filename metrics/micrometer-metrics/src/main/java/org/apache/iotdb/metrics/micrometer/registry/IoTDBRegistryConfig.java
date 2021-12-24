package org.apache.iotdb.metrics.micrometer.registry;

import io.micrometer.core.instrument.step.StepRegistryConfig;

public interface IoTDBRegistryConfig extends StepRegistryConfig {
    IoTDBRegistryConfig DEFAULT = k -> null;
    @Override
    default String prefix(){
        return "iotdb";
    }
}
