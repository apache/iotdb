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

package org.apache.iotdb.influxdb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class IoTDBInfluxDBUtilsTest {

  @Test
  public void testGetSame() {
    ArrayList<String> columns = new ArrayList<>();
    columns.addAll(Arrays.asList("time", "root.111.1", "root.111.2", "root.222.1"));
    ArrayList<Integer> list =
        IoTDBInfluxDBUtils.getSamePathForList(columns.subList(1, columns.size()));
    assert list.get(0) == 1;
    assert list.get(1) == 2;
    assert list.size() == 2;
  }
}
