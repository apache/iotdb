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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.mqtt;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** PayloadFormatManager loads payload formatter from SPI services. */
public class PayloadFormatManager {
  private static Map<String, PayloadFormatter> map = new HashMap<>();

  static {
    init();
  }

  private static void init() {
    ServiceLoader<PayloadFormatter> formats = ServiceLoader.load(PayloadFormatter.class);
    for (PayloadFormatter format : formats) {
      map.put(format.getName(), format);
    }
  }

  public static PayloadFormatter getPayloadFormat(String name) {
    Preconditions.checkArgument(map.containsKey(name), "Unknown payload format named: " + name);
    return map.get(name);
  }
}
