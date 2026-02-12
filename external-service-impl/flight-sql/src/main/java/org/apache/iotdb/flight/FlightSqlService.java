/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.flight;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.externalservice.api.IExternalService;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Arrow Flight SQL service implementation for IoTDB. Implements the IExternalService interface to
 * integrate with IoTDB's external service management framework (plugin lifecycle).
 *
 * <p>This service starts a gRPC-based Arrow Flight SQL server that allows clients to execute SQL
 * queries using the Arrow Flight SQL protocol and receive results in columnar Arrow format.
 */
public class FlightSqlService implements IExternalService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlService.class);
  private static final long SESSION_TIMEOUT_MINUTES = 30;

  private FlightServer flightServer;
  private BufferAllocator allocator;
  private FlightSqlSessionManager flightSessionManager;
  private IoTDBFlightSqlProducer producer;

  @Override
  public void start() {
    int port = IoTDBDescriptor.getInstance().getConfig().getArrowFlightSqlPort();
    LOGGER.info("Starting Arrow Flight SQL service on port {}", port);

    try {
      // Create the root allocator for Arrow memory management
      allocator = new RootAllocator(Long.MAX_VALUE);

      // Create session manager with TTL
      flightSessionManager = new FlightSqlSessionManager(SESSION_TIMEOUT_MINUTES);

      // Create the auth handler
      FlightSqlAuthHandler authHandler = new FlightSqlAuthHandler(flightSessionManager);

      // Create the Flight SQL producer
      producer = new IoTDBFlightSqlProducer(allocator, flightSessionManager);

      // Build the Flight server with auth2 Bearer token authentication
      Location location = Location.forGrpcInsecure("0.0.0.0", port);
      flightServer =
          FlightServer.builder(allocator, location, producer)
              .headerAuthenticator(
                  new GeneratedBearerTokenAuthenticator(
                      new BasicCallHeaderAuthenticator(authHandler)))
              .build();

      flightServer.start();
      LOGGER.info(
          "Arrow Flight SQL service started successfully on port {}", flightServer.getPort());
    } catch (IOException e) {
      LOGGER.error("Failed to start Arrow Flight SQL service", e);
      stop();
      throw new RuntimeException("Failed to start Arrow Flight SQL service", e);
    }
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping Arrow Flight SQL service");

    if (flightServer != null) {
      try {
        flightServer.shutdown();
        flightServer.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for Flight server shutdown", e);
        Thread.currentThread().interrupt();
        try {
          flightServer.close();
        } catch (Exception ex) {
          LOGGER.warn("Error force-closing Flight server", ex);
        }
      } catch (Exception e) {
        LOGGER.warn("Error shutting down Flight server", e);
      }
      flightServer = null;
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing Flight SQL producer", e);
      }
      producer = null;
    }

    if (flightSessionManager != null) {
      flightSessionManager.close();
      flightSessionManager = null;
    }

    if (allocator != null) {
      allocator.close();
      allocator = null;
    }

    LOGGER.info("Arrow Flight SQL service stopped");
  }
}
