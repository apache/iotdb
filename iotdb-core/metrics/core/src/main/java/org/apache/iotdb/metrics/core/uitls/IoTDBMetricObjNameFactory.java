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

package org.apache.iotdb.metrics.core.uitls;

import org.apache.iotdb.metrics.core.reporter.IoTDBJmxReporter;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import com.codahale.metrics.jmx.ObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Hashtable;
import java.util.Map;
import java.util.stream.Collectors;

public class IoTDBMetricObjNameFactory implements ObjectNameFactory {
  private static final String TAG_SEPARATOR = ".";
  private static final char[] QUOTABLE_CHARS = new char[] {',', '=', ':', '"'};
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBJmxReporter.class);

  private IoTDBMetricObjNameFactory() {
    // util class
  }

  /**
   * Create objectName for a certain metric.
   *
   * @param type metric type
   * @param domain metric domain
   * @param name metric name
   * @return metric's objectName
   */
  @Override
  public ObjectName createName(String type, String domain, String name) {
    try {
      ObjectName objectName;
      Hashtable<String, String> properties = new Hashtable<>();

      properties.put("name", name);
      properties.put("type", type);
      objectName = new ObjectName(domain, properties);

      /*
       * The only way we can find out if we need to quote the properties is by
       * checking an ObjectName that we've constructed.
       */
      if (objectName.isDomainPattern()) {
        domain = ObjectName.quote(domain);
      }
      if (objectName.isPropertyValuePattern("name")
          || shouldQuote(objectName.getKeyProperty("name"))) {
        properties.put("name", ObjectName.quote(name));
      }
      if (objectName.isPropertyValuePattern("type")
          || shouldQuote(objectName.getKeyProperty("type"))) {
        properties.put("type", ObjectName.quote(type));
      }
      objectName = new ObjectName(domain, properties);

      return objectName;
    } catch (MalformedObjectNameException e) {
      try {
        return new ObjectName(domain, "name", ObjectName.quote(name));
      } catch (MalformedObjectNameException e1) {
        LOGGER.warn("IoTDB Metric: Unable to register {} {}", type, name, e1);
        throw new RuntimeException(e1);
      }
    }
  }

  /**
   * Determines whether the value requires quoting. According to the {@link ObjectName}
   * documentation, values can be quoted or unquoted. Unquoted values may not contain any of the
   * characters comma, equals, colon, or quote.
   *
   * @param value a value to test
   * @return true when it requires quoting, false otherwise
   */
  private boolean shouldQuote(final String value) {
    for (char quotableChar : QUOTABLE_CHARS) {
      if (value.indexOf(quotableChar) != -1) {
        return true;
      }
    }
    return false;
  }

  /**
   * Transform flat string and metric type to metricInfo.
   *
   * @param metricType the type of metric
   * @param flatString the flat string of metricInfo
   */
  public static MetricInfo transformFromString(MetricType metricType, String flatString) {
    MetricInfo metricInfo;
    String name;
    int firstIndex = flatString.indexOf("{");
    int lastIndex = flatString.indexOf("}");
    if (firstIndex == -1 || lastIndex == -1) {
      name = flatString.replaceAll("[^a-zA-Z0-9:_\\]\\[]", "_");
      metricInfo = new MetricInfo(metricType, name);
    } else {
      name = flatString.substring(0, firstIndex).replaceAll("[^a-zA-Z0-9:_\\]\\[]", "_");
      String tagsPart = flatString.substring(firstIndex + 1, lastIndex);
      if (0 == tagsPart.length()) {
        metricInfo = new MetricInfo(metricType, name);
      } else {
        metricInfo = new MetricInfo(metricType, name, tagsPart.split("\\."));
      }
    }
    return metricInfo;
  }

  /**
   * Transform metricInfo to flat string.
   *
   * @param metricInfo the info of metric
   */
  public static String toFlatString(MetricInfo metricInfo) {
    String name = metricInfo.getName();
    Map<String, String> tags = metricInfo.getTags();
    return name.replace("{", "").replace("}", "")
        + "{"
        + tags.entrySet().stream()
            .map(
                t ->
                    t.getKey().replace(TAG_SEPARATOR, "")
                        + TAG_SEPARATOR
                        + t.getValue().replace(TAG_SEPARATOR, ""))
            .collect(Collectors.joining(TAG_SEPARATOR))
            .replace("{", "")
            .replace("}", "")
        + "}";
  }

  private static class IoTDBMetricObjNameFactoryHolder {
    private static final IoTDBMetricObjNameFactory INSTANCE = new IoTDBMetricObjNameFactory();

    private IoTDBMetricObjNameFactoryHolder() {
      // empty constructor
    }
  }

  public static IoTDBMetricObjNameFactory getInstance() {
    return IoTDBMetricObjNameFactoryHolder.INSTANCE;
  }
}
