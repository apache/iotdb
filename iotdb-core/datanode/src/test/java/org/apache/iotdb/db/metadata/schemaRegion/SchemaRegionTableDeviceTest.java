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

package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.schema.filter.impl.DeviceAttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceIdFilter;
import org.apache.iotdb.commons.schema.filter.impl.OrFilter;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaRegionTableDeviceTest extends AbstractSchemaRegionTest {

  public SchemaRegionTableDeviceTest(SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Test
  public void testDeviceCreation() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    String tableName = "t";
    List<String[]> deviceIdList =
        Arrays.asList(
            new String[] {"hebei", "p_1", "d_0"},
            new String[] {"hebei", "p_1", "d_1"},
            new String[] {"shandong", "p_1", "d_1"});

    for (String[] deviceId : deviceIdList) {
      SchemaRegionTestUtil.createTableDevice(
          schemaRegion, tableName, deviceId, Collections.emptyMap());
    }
    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName, deviceIdList);
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    List<String[]> result =
        deviceSchemaInfoList.stream()
            .map(IDeviceSchemaInfo::getRawNodes)
            .collect(Collectors.toList());
    Assert.assertEquals(
        deviceIdList.stream().map(Arrays::toString).sorted().collect(Collectors.toList()),
        result.stream()
            .map(o -> Arrays.copyOfRange(o, 3, o.length))
            .map(Arrays::toString)
            .sorted()
            .collect(Collectors.toList()));

    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"shandong", "p_1", "d_1"}, attributeMap);

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            Collections.singletonList(new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertEquals("new", deviceSchemaInfoList.get(0).getAttributeValue("type"));
    Assert.assertEquals("monthly", deviceSchemaInfoList.get(0).getAttributeValue("cycle"));

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            Collections.singletonList(new String[] {"hebei", "p_1", "d_1"}));
    Assert.assertEquals("old", deviceSchemaInfoList.get(0).getAttributeValue("type"));
    Assert.assertEquals("monthly", deviceSchemaInfoList.get(0).getAttributeValue("cycle"));

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            Collections.singletonList(new String[] {"shandong", "p_1", "d_1"}));
    Assert.assertEquals("old", deviceSchemaInfoList.get(0).getAttributeValue("type"));
    Assert.assertEquals("daily", deviceSchemaInfoList.get(0).getAttributeValue("cycle"));
  }

  @Test
  public void testDeviceQuery() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    String tableName = "t";

    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"shandong", "p_1", "d_1"}, attributeMap);

    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Arrays.asList(new DeviceIdFilter(0, "hebei"), new DeviceIdFilter(1, "p_1")),
            null);
    Assert.assertEquals(2, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(new DeviceIdFilter(1, "p_1")),
            null);
    Assert.assertEquals(3, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(new DeviceIdFilter(1, "p_1")),
            new DeviceAttributeFilter("cycle", "daily"));
    Assert.assertEquals(1, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.emptyList(),
            new OrFilter(
                new DeviceIdFilter(1, "p_1"), new DeviceAttributeFilter("cycle", "daily")));
    Assert.assertEquals(3, deviceSchemaInfoList.size());
  }

  @Test
  public void testDeviceIdWithNull() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    String tableName = "t";

    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", null);
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", null, "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"shandong", "p_1", null}, attributeMap);

    List<String[]> deviceIdList =
        Arrays.asList(
            new String[] {"hebei", null, "d_0"},
            new String[] {"hebei", "p_1", "d_1"},
            new String[] {"shandong", "p_1", null});
    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName, deviceIdList);
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    Assert.assertEquals(
        deviceIdList.stream().map(Arrays::toString).sorted().collect(Collectors.toList()),
        deviceSchemaInfoList.stream()
            .map(o -> Arrays.copyOfRange(o.getRawNodes(), 3, o.getRawNodes().length))
            .map(Arrays::toString)
            .sorted()
            .collect(Collectors.toList()));

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(new DeviceIdFilter(1, null)),
            null);
    Assert.assertEquals(1, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.emptyList(),
            new OrFilter(new DeviceIdFilter(2, null), new DeviceAttributeFilter("cycle", null)));
    Assert.assertEquals(3, deviceSchemaInfoList.size());
  }

  @Test
  public void testDeviceWithDifferentIdLength() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    String tableName = "t";

    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"shandong", "p_1", "d_1", "r_1"}, attributeMap);

    List<String[]> deviceIdList =
        Arrays.asList(
            new String[] {"hebei", "p_1", "d_0"},
            new String[] {"hebei", "p_1", "d_1"},
            new String[] {"shandong", "p_1", "d_1", "r_1"});
    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName, deviceIdList);
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    Assert.assertEquals(
        deviceIdList.stream().map(Arrays::toString).sorted().collect(Collectors.toList()),
        deviceSchemaInfoList.stream()
            .map(o -> Arrays.copyOfRange(o.getRawNodes(), 3, o.getRawNodes().length))
            .map(Arrays::toString)
            .sorted()
            .collect(Collectors.toList()));

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            4,
            Collections.singletonList(new DeviceIdFilter(3, "r_1")),
            null);
    Assert.assertEquals(1, deviceSchemaInfoList.size());

    // todo implement device query after table column extension
    //    deviceSchemaInfoList =
    //            SchemaRegionTestUtil.getTableDevice(
    //                    schemaRegion,
    //                    tableName,
    //                    4,
    //                    Collections.singletonList(new DeviceIdFilter(3, null)),
    //                    null);
    //    Assert.assertEquals(2, deviceSchemaInfoList.size());
  }

  @Test
  public void testMultiTableDevice() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    String tableName1 = "t1";

    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName1, new String[] {"hebei", "p_1", "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName1, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName1, new String[] {"shandong", "p_1", "d_1", "r_1"}, attributeMap);

    String tableName2 = "t2";

    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName2, new String[] {"hebei", "p_1", "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName2, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName2, new String[] {"shandong", "p_1", "d_1", "r_1"}, attributeMap);

    List<String[]> deviceIdList =
        Arrays.asList(
            new String[] {"hebei", "p_1", "d_0"},
            new String[] {"hebei", "p_1", "d_1"},
            new String[] {"shandong", "p_1", "d_1", "r_1"});
    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName1, deviceIdList);
    Assert.assertEquals(3, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName2, deviceIdList);
    Assert.assertEquals(3, deviceSchemaInfoList.size());
  }
}
