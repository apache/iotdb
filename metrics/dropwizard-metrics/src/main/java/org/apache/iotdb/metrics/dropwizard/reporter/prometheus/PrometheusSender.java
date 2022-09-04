/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.dropwizard.reporter.prometheus;

import com.codahale.metrics.*;

import java.io.Closeable;
import java.io.IOException;

public interface PrometheusSender extends Closeable {

  /**
   * Connects to the server.
   *
   * @throws IllegalStateException if the client is already connected
   * @throws IOException if there is an error connecting
   */
  void connect() throws IllegalStateException, IOException;

  void sendGauge(String name, Gauge<?> gauge) throws IOException;

  void sendCounter(String name, Counter counter) throws IOException;

  void sendHistogram(String name, Histogram histogram) throws IOException;

  void sendMeter(String name, Meter meter) throws IOException;

  void sendTimer(String name, Timer timer) throws IOException;

  /**
   * Flushes buffer, if applicable
   *
   * @throws IOException if there is an error connecting
   */
  void flush() throws IOException;

  /**
   * Returns true if ready to send data
   *
   * @return connection status
   */
  boolean isConnected();

  /** DisConnect */
  void disConnect();
}
