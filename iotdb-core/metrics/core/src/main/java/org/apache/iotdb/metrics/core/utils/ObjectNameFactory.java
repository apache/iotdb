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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.util.Hashtable;
import java.util.Map;

public interface ObjectNameFactory {
  /**
   * Create objectName for a certain metric.
   *
   * @param type metric type
   * @param domain metric domain
   * @param name metric name
   * @return metric's objectName
   */
  public ObjectName createName(String type, String domain, String name);

  /**
   * Include all metric tags in the MBean identity. Tag keys use a separate namespace and reversible
   * escaping so they cannot overwrite the metric name/type or collide after sanitization. Tag
   * values are quoted to preserve literal wildcard characters. Untagged metrics keep their existing
   * names.
   */
  default ObjectName createName(String type, String domain, String name, Map<String, String> tags) {
    ObjectName base = createName(type, domain, name);
    if (tags.isEmpty()) {
      return base;
    }
    Hashtable<String, String> properties = base.getKeyPropertyList();
    tags.forEach((key, value) -> properties.put(encodeTagKey(key), ObjectName.quote(value)));
    try {
      return new ObjectName(base.getDomain(), properties);
    } catch (MalformedObjectNameException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static String encodeTagKey(String key) {
    StringBuilder encoded = new StringBuilder("tag.");
    for (int i = 0; i < key.length(); i++) {
      char character = key.charAt(i);
      if ((character >= 'a' && character <= 'z')
          || (character >= 'A' && character <= 'Z')
          || (character >= '0' && character <= '9')
          || character == '_'
          || character == '-'
          || character == '.') {
        encoded.append(character);
      } else {
        encoded.append('%');
        for (int shift = 12; shift >= 0; shift -= 4) {
          encoded.append(Character.forDigit((character >> shift) & 0xf, 16));
        }
      }
    }
    return encoded.toString();
  }
}
