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

package org.apache.iotdb.metrics.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.util.Hashtable;

public class IoTDBMetricObjNameFactory implements ObjectNameFactory {
  private static final char[] QUOTABLE_CHARS = new char[] {',', '=', ':', '"'};
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBMetricObjNameFactory.class);

  private IoTDBMetricObjNameFactory() {
    // util class
  }

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
