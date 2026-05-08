/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metricscrape;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class MetricScrapeHttpClient {

  public String get(String url, int timeoutMs) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(timeoutMs);
    connection.setReadTimeout(timeoutMs);
    connection.setRequestProperty(
        "Accept", "text/plain; version=0.0.4, application/openmetrics-text, */*");
    connection.setRequestProperty("User-Agent", "IoTDB-MetricScrapeService");

    int statusCode = connection.getResponseCode();
    if (statusCode < 200 || statusCode >= 300) {
      throw new IOException("Metric scrape target " + url + " returns HTTP " + statusCode);
    }
    try (InputStream inputStream = connection.getInputStream()) {
      return readToString(inputStream);
    } finally {
      connection.disconnect();
    }
  }

  private String readToString(InputStream inputStream) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, length);
    }
    return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
  }
}
