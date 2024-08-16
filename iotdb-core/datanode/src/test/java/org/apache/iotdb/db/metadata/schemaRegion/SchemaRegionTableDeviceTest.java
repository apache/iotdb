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

import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.NotFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.LikeFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tsfile.utils.RegexUtils.parseLikePatternToRegex;

public class SchemaRegionTableDeviceTest extends AbstractSchemaRegionTest {

  public SchemaRegionTableDeviceTest(final SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Test
  public void testDeviceCreation() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    final ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    final String tableName = "t";
    final List<String[]> deviceIdList =
        Arrays.asList(
            new String[] {"hebei", "p_1", "d_0"},
            new String[] {"hebei", "p_1", "d_1"},
            new String[] {"shandong", "p_1", "d_1"});

    for (final String[] deviceId : deviceIdList) {
      SchemaRegionTestUtil.createTableDevice(
          schemaRegion, tableName, deviceId, Collections.emptyMap());
    }
    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName, deviceIdList);
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    final List<String[]> result =
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

    final Map<String, String> attributeMap = new HashMap<>();
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
    final ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    final String tableName = "t";

    final Map<String, String> attributeMap = new HashMap<>();
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
            Arrays.asList(
                new IdFilter(new PreciseFilter("hebei"), 0),
                new IdFilter(new PreciseFilter("p_1"), 1)));
    Assert.assertEquals(2, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(new IdFilter(new PreciseFilter("p_1"), 1)));
    Assert.assertEquals(3, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(new IdFilter(new InFilter(Collections.singleton("d_1")), 2)));

    Assert.assertEquals(2, deviceSchemaInfoList.size());

    // Test multi filters on one id
    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Arrays.asList(
                new IdFilter(new InFilter(new HashSet<>(Arrays.asList("d_0", "d_1"))), 2),
                new IdFilter(new LikeFilter(parseLikePatternToRegex("__1")), 2)));

    Assert.assertEquals(2, deviceSchemaInfoList.size());
  }

  @Test
  public void testDeviceIdWithNull() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    final ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    final String tableName = "t";

    final Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", null);
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", null, "d_0"}, attributeMap);
    attributeMap.put("type", "old");
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"hebei", "p_1", "d_1"}, attributeMap);
    attributeMap.put("cycle", "daily");
    // The null suffix is trimmed
    SchemaRegionTestUtil.createTableDevice(
        schemaRegion, tableName, new String[] {"shandong", "p_1"}, attributeMap);
    // Pure null
    SchemaRegionTestUtil.createTableDevice(schemaRegion, tableName, new String[] {}, attributeMap);

    final List<String[]> deviceIdList =
        Arrays.asList(
            new String[] {"hebei", null, "d_0"},
            new String[] {"hebei", "p_1", "d_1"},
            new String[] {"shandong", "p_1"},
            new String[] {});
    List<IDeviceSchemaInfo> deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(schemaRegion, tableName, deviceIdList);
    Assert.assertEquals(4, deviceSchemaInfoList.size());
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
            Collections.singletonList(new IdFilter(new PreciseFilter((String) null), 0)));
    Assert.assertEquals(1, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(new IdFilter(new PreciseFilter((String) null), 1)));
    Assert.assertEquals(2, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(
                new IdFilter(new NotFilter(new PreciseFilter((String) null)), 2)));

    Assert.assertEquals(2, deviceSchemaInfoList.size());

    deviceSchemaInfoList =
        SchemaRegionTestUtil.getTableDevice(
            schemaRegion,
            tableName,
            3,
            Collections.singletonList(
                new IdFilter(new LikeFilter(parseLikePatternToRegex("%")), 2)));

    Assert.assertEquals(2, deviceSchemaInfoList.size());
  }

  @Test
  public void testDeviceWithDifferentIdLength() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    final ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    final String tableName = "t";

    final Map<String, String> attributeMap = new HashMap<>();
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

    final List<String[]> deviceIdList =
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
            Collections.singletonList(new IdFilter(new PreciseFilter("r_1"), 3)));
    Assert.assertEquals(1, deviceSchemaInfoList.size());
  }

  @Test
  public void testMultiTableDevice() throws Exception {
    if (!testParams.getTestModeName().equals("MemoryMode")) {
      return;
    }
    final ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    final String tableName1 = "t1";

    final Map<String, String> attributeMap = new HashMap<>();
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

    final List<String[]> deviceIdList =
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
