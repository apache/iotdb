/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metrics2;

import com.sun.net.httpserver.HttpServer;
import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Metrics Service that is based on micrometer.
 * It does two things.
 * First, exposes a Prometheus Endpoint on :8080/metrics.
 * Second, it logs all collected metrics into IoTDB in the storage group root._metrics.
 */
public class MicrometerServerService implements IService {

    private static final MicrometerServerService INSTANCE = new MicrometerServerService();

    private HttpServer server;
    private final PrometheusMeterRegistry prometheusMeterRegistry;

    public MicrometerServerService() {
        prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    public static IService getInstance() {
        return INSTANCE;
    }

    @Override
    public void start() throws StartupException {
        // Define Meter Registry
        MeterRegistry registry = new CompositeMeterRegistry(Clock.SYSTEM,
            Arrays.asList(new IoTDBRegistry(IoTDBRegistryConfig.DEFAULT, Clock.SYSTEM), prometheusMeterRegistry));
        // Set this as default, then users can simply write Metrics.xxx
        Metrics.addRegistry(registry);
        // Wire up JVM and Other Default Bindings
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessMemoryMetrics().bindTo(registry);
        new ProcessThreadMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
        new FileDescriptorMetrics().bindTo(registry);

        // Serve an Endpoint for prometheus
        try {
            server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusMeterRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    @Override
    public ServiceType getID() {
        return ServiceType.METRICS2_SERVICE;
    }
}
