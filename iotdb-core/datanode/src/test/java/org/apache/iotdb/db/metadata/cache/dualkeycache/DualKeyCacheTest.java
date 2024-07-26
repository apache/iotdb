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

package org.apache.iotdb.db.metadata.cache.dualkeycache;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.SchemaCacheEntry;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheComputation;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheUpdating;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.DataNodeLastCacheManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class DualKeyCacheTest {

  private final String policy;

  public DualKeyCacheTest(String policy) {
    this.policy = policy;
  }

  @Parameterized.Parameters
  public static List<String> getTestModes() {
    return Arrays.asList("FIFO", "LRU");
  }

  @Test
  public void testBasicReadPut() {
    DualKeyCacheBuilder<String, String, String> dualKeyCacheBuilder = new DualKeyCacheBuilder<>();
    IDualKeyCache<String, String, String> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.valueOf(policy))
            .memoryCapacity(300)
            .firstKeySizeComputer(this::computeStringSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(this::computeStringSize)
            .build();

    String[] firstKeyList = new String[] {"root.db.d1", "root.db.d2"};
    String[] secondKeyList = new String[] {"s1", "s2"};
    String[][] valueTable =
        new String[][] {new String[] {"1-1", "1-2"}, new String[] {"2-1", "2-2"}};

    for (int i = 0; i < firstKeyList.length; i++) {
      for (int j = 0; j < secondKeyList.length; j++) {
        dualKeyCache.put(firstKeyList[i], secondKeyList[j], valueTable[i][j]);
      }
    }

    int firstKeyOfMissingEntry = -1, secondKeyOfMissingEntry = -1;
    for (int i = 0; i < firstKeyList.length; i++) {
      for (int j = 0; j < secondKeyList.length; j++) {
        String value = dualKeyCache.get(firstKeyList[i], secondKeyList[j]);
        if (value == null) {
          if (firstKeyOfMissingEntry == -1) {
            firstKeyOfMissingEntry = i;
            secondKeyOfMissingEntry = j;
          } else {
            Assert.fail();
          }
        } else {
          Assert.assertEquals(valueTable[i][j], value);
        }
      }
    }

    Assert.assertEquals(230, dualKeyCache.stats().memoryUsage());
    Assert.assertEquals(4, dualKeyCache.stats().requestCount());
    Assert.assertEquals(3, dualKeyCache.stats().hitCount());

    dualKeyCache.put(
        firstKeyList[firstKeyOfMissingEntry],
        secondKeyList[secondKeyOfMissingEntry],
        valueTable[firstKeyOfMissingEntry][secondKeyOfMissingEntry]);
    Assert.assertEquals(230, dualKeyCache.stats().memoryUsage());

    for (int i = 0; i < firstKeyList.length; i++) {
      int finalI = i;
      dualKeyCache.compute(
          new IDualKeyCacheComputation<String, String, String>() {
            @Override
            public String getFirstKey() {
              return firstKeyList[finalI];
            }

            @Override
            public String[] getSecondKeyList() {
              return secondKeyList;
            }

            @Override
            public void computeValue(int index, String value) {
              if (value != null) {
                Assert.assertEquals(valueTable[finalI][index], value);
              }
            }
          });
    }

    Assert.assertEquals(8, dualKeyCache.stats().requestCount());
    Assert.assertEquals(6, dualKeyCache.stats().hitCount());
  }

  private int computeStringSize(String string) {
    return 8 + 8 + 4 + 2 * string.length();
  }

  @Test
  public void testComputeAndUpdateSize() {
    DualKeyCacheBuilder<String, String, SchemaCacheEntry> dualKeyCacheBuilder =
        new DualKeyCacheBuilder<>();
    IDualKeyCache<String, String, SchemaCacheEntry> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.valueOf(policy))
            .memoryCapacity(500)
            .firstKeySizeComputer(this::computeStringSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(SchemaCacheEntry::estimateSize)
            .build();
    String firstKey = "root.db.d1";
    String[] secondKeyList = new String[] {"s1", "s2"};
    for (String s : secondKeyList) {
      dualKeyCache.put(
          firstKey,
          s,
          new SchemaCacheEntry(
              "root.db",
              new MeasurementSchema(s, TSDataType.INT32),
              Collections.emptyMap(),
              false));
    }
    SchemaCacheEntry schemaCacheEntry =
        new SchemaCacheEntry(
            "root.db",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false);
    int expectedSize =
        computeStringSize("root.db.d1")
            + computeStringSize("s1") * 2
            + SchemaCacheEntry.estimateSize(schemaCacheEntry) * 2;
    Assert.assertEquals(expectedSize, dualKeyCache.stats().memoryUsage());
    dualKeyCache.update(
        new IDualKeyCacheUpdating<String, String, SchemaCacheEntry>() {
          @Override
          public String getFirstKey() {
            return firstKey;
          }

          @Override
          public String[] getSecondKeyList() {
            return secondKeyList;
          }

          @Override
          public int updateValue(int index, SchemaCacheEntry value) {
            return DataNodeLastCacheManager.updateLastCache(
                value, new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
          }
        });
    int tmp = SchemaCacheEntry.estimateSize(schemaCacheEntry);
    schemaCacheEntry.updateLastCache(new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
    expectedSize += (SchemaCacheEntry.estimateSize(schemaCacheEntry) - tmp) * 2;
    Assert.assertEquals(expectedSize, dualKeyCache.stats().memoryUsage());
  }

  @Test
  public void testInvalidDatabase() throws IllegalPathException {
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache = generateCache();
    dualKeyCache.invalidate("root.db1");
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s1"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s2"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1"), "s11"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s2"));
    int expectSize =
        PartialPath.estimateSize(new PartialPath("root.db2.d1"))
            + computeStringSize("s1") * 2
            + SchemaCacheEntry.estimateSize(
                    new SchemaCacheEntry(
                        "root.db1",
                        new MeasurementSchema("s1", TSDataType.INT32),
                        Collections.emptyMap(),
                        false))
                * 2;
    Assert.assertEquals(expectSize, dualKeyCache.stats().memoryUsage());
    dualKeyCache.evictOneEntry();
    expectSize =
        PartialPath.estimateSize(new PartialPath("root.db2.d1"))
            + computeStringSize("s1")
            + SchemaCacheEntry.estimateSize(
                new SchemaCacheEntry(
                    "root.db1",
                    new MeasurementSchema("s1", TSDataType.INT32),
                    Collections.emptyMap(),
                    false));
    Assert.assertEquals(expectSize, dualKeyCache.stats().memoryUsage());
    dualKeyCache.evictOneEntry();
    Assert.assertEquals(0, dualKeyCache.stats().memoryUsage());
  }

  @Test
  public void testInvalidPathPattern1() throws IllegalPathException {
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache = generateCache();
    dualKeyCache.invalidate(
        Arrays.asList(new MeasurementPath("root.db2.**"), new MeasurementPath("root.db1.d1.s1")));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s2"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1"), "s11"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s1"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s2"));
    int expectSize =
        PartialPath.estimateSize(new PartialPath("root.db1.d1"))
            + PartialPath.estimateSize(new PartialPath("root.db1"))
            + computeStringSize("s1")
            + computeStringSize("s11")
            + SchemaCacheEntry.estimateSize(
                new SchemaCacheEntry(
                    "root.db1",
                    new MeasurementSchema("s1", TSDataType.INT32),
                    Collections.emptyMap(),
                    false))
            + SchemaCacheEntry.estimateSize(
                new SchemaCacheEntry(
                    "root.db1",
                    new MeasurementSchema("s11", TSDataType.INT32),
                    Collections.emptyMap(),
                    false));
    Assert.assertEquals(expectSize, dualKeyCache.stats().memoryUsage());
    dualKeyCache.evictOneEntry();
    dualKeyCache.evictOneEntry();
    Assert.assertEquals(0, dualKeyCache.stats().memoryUsage());
  }

  @Test
  public void testInvalidPathPattern2() throws IllegalPathException {
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache = generateCache();
    dualKeyCache.invalidate(
        Arrays.asList(new MeasurementPath("root.db1.**"), new MeasurementPath("root.db2.d1.*1")));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s1"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s2"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db1"), "s11"));
    Assert.assertNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s2"));
    int expectSize =
        PartialPath.estimateSize(new PartialPath("root.db2.d1"))
            + computeStringSize("s2")
            + SchemaCacheEntry.estimateSize(
                new SchemaCacheEntry(
                    "root.db2",
                    new MeasurementSchema("s2", TSDataType.INT32),
                    Collections.emptyMap(),
                    false));
    Assert.assertEquals(expectSize, dualKeyCache.stats().memoryUsage());
    dualKeyCache.evictOneEntry();
    Assert.assertEquals(0, dualKeyCache.stats().memoryUsage());
  }

  private IDualKeyCache<PartialPath, String, SchemaCacheEntry> generateCache()
      throws IllegalPathException {
    DualKeyCacheBuilder<PartialPath, String, SchemaCacheEntry> dualKeyCacheBuilder =
        new DualKeyCacheBuilder<>();
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.valueOf(policy))
            .memoryCapacity(2000) // actual threshold is 1600
            .firstKeySizeComputer(PartialPath::estimateSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(SchemaCacheEntry::estimateSize)
            .build();
    dualKeyCache.put(
        new PartialPath("root.db1.d1"),
        "s1",
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false));
    dualKeyCache.put(
        new PartialPath("root.db1.d1"),
        "s2",
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false));
    dualKeyCache.put(
        new PartialPath("root.db1"),
        "s11",
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s11", TSDataType.INT32),
            Collections.emptyMap(),
            false));
    dualKeyCache.put(
        new PartialPath("root.db1.d1"),
        "s2",
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false));
    dualKeyCache.put(
        new PartialPath("root.db2.d1"),
        "s1",
        new SchemaCacheEntry(
            "root.db2",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false));
    dualKeyCache.put(
        new PartialPath("root.db2.d1"),
        "s2",
        new SchemaCacheEntry(
            "root.db2",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s2"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1"), "s11"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s2"));
    int expectSize =
        PartialPath.estimateSize(new PartialPath("root.db1.d1")) * 2
            + PartialPath.estimateSize(new PartialPath("root.db1"))
            + computeStringSize("s1") * 4
            + computeStringSize("s11")
            + SchemaCacheEntry.estimateSize(
                    new SchemaCacheEntry(
                        "root.db1",
                        new MeasurementSchema("s1", TSDataType.INT32),
                        Collections.emptyMap(),
                        false))
                * 4
            + SchemaCacheEntry.estimateSize(
                new SchemaCacheEntry(
                    "root.db1",
                    new MeasurementSchema("s11", TSDataType.INT32),
                    Collections.emptyMap(),
                    false));
    Assert.assertEquals(expectSize, dualKeyCache.stats().memoryUsage());
    return dualKeyCache;
  }

  private IDualKeyCache<PartialPath, String, SchemaCacheEntry> generateLastCache()
      throws IllegalPathException {
    DualKeyCacheBuilder<PartialPath, String, SchemaCacheEntry> dualKeyCacheBuilder =
        new DualKeyCacheBuilder<>();
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.valueOf(policy))
            .memoryCapacity(2000) // actual threshold is 1600
            .firstKeySizeComputer(PartialPath::estimateSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(SchemaCacheEntry::estimateSize)
            .build();
    SchemaCacheEntry cacheEntry1 =
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s1", TSDataType.INT32),
            Collections.emptyMap(),
            false);
    cacheEntry1.updateLastCache(new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
    dualKeyCache.put(new PartialPath("root.db1.d1"), "s1", cacheEntry1);

    SchemaCacheEntry cacheEntry2 =
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s2", TSDataType.INT32),
            Collections.emptyMap(),
            false);
    cacheEntry2.updateLastCache(new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
    dualKeyCache.put(new PartialPath("root.db1.d1"), "s2", cacheEntry2);

    SchemaCacheEntry cacheEntry3 =
        new SchemaCacheEntry(
            "root.db2",
            new MeasurementSchema("s2", TSDataType.INT32),
            Collections.emptyMap(),
            false);
    cacheEntry3.updateLastCache(new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
    dualKeyCache.put(new PartialPath("root.db2.d1"), "s2", cacheEntry3);

    SchemaCacheEntry cacheEntry4 =
        new SchemaCacheEntry(
            "root.db2",
            new MeasurementSchema("s2", TSDataType.INT32),
            Collections.emptyMap(),
            false);
    cacheEntry4.updateLastCache(new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
    dualKeyCache.put(new PartialPath("root.db2.d1"), "s1", cacheEntry4);

    SchemaCacheEntry cacheEntry5 =
        new SchemaCacheEntry(
            "root.db1",
            new MeasurementSchema("s2", TSDataType.INT32),
            Collections.emptyMap(),
            false);
    cacheEntry5.updateLastCache(new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), true, 0L);
    dualKeyCache.put(new PartialPath("root.db1"), "s2", cacheEntry5);

    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1.d1"), "s2"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db1"), "s2"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s1"));
    Assert.assertNotNull(dualKeyCache.get(new PartialPath("root.db2.d1"), "s2"));

    int expectSize =
        PartialPath.estimateSize(new PartialPath("root.db1.d1")) * 2
            + PartialPath.estimateSize(new PartialPath("root.db1"))
            + computeStringSize("s1") * 5
            + SchemaCacheEntry.estimateSize(cacheEntry1) * 5;
    Assert.assertEquals(expectSize, dualKeyCache.stats().memoryUsage());
    return dualKeyCache;
  }

  @Test
  public void testInvalidateSimpleTimeseriesAndDataRegion() throws IllegalPathException {
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache = generateLastCache();
    long memUse = dualKeyCache.stats().memoryUsage();

    dualKeyCache.invalidateLastCache(new MeasurementPath("root.db1.d1.s1"));
    SchemaCacheEntry cacheEntry = dualKeyCache.get(new PartialPath("root.db1.d1"), "s1");
    Assert.assertNull(cacheEntry.getLastCacheContainer().getCachedLast());

    dualKeyCache.invalidateLastCache(new MeasurementPath("root.db1.d1.*"));
    cacheEntry = dualKeyCache.get(new PartialPath("root.db1.d1"), "s2");
    Assert.assertNull(cacheEntry.getLastCacheContainer().getCachedLast());

    dualKeyCache.invalidateLastCache(new MeasurementPath("root.db2.d1.**"));
    cacheEntry = dualKeyCache.get(new PartialPath("root.db2.d1"), "s2");
    Assert.assertNull(cacheEntry.getLastCacheContainer().getCachedLast());

    dualKeyCache.invalidateDataRegionLastCache("root.db2");
    cacheEntry = dualKeyCache.get(new PartialPath("root.db2.d1"), "s1");
    Assert.assertNull(cacheEntry.getLastCacheContainer().getCachedLast());

    cacheEntry = dualKeyCache.get(new PartialPath("root.db1"), "s2");
    // last cache container' estimateSize(): header 8b + Ilastcachevalueref 8b +  lastcache's size
    // invalidate operation: make Ilastcachevalueref = null.
    // So the amount of change in size is estimateSize() - 8b - 8b
    int size = cacheEntry.getLastCacheContainer().estimateSize() - 16;
    Assert.assertEquals(memUse - size * 4, dualKeyCache.stats().memoryUsage());
  }

  @Test
  public void testComplexInvalidate() throws IllegalPathException {
    IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache = generateLastCache();

    dualKeyCache.invalidateLastCache(new MeasurementPath("root.db1.*.s1"));
    SchemaCacheEntry cacheEntry = dualKeyCache.get(new PartialPath("root.db1.d1"), "s1");
    Assert.assertNull(cacheEntry.getLastCacheContainer().getCachedLast());

    dualKeyCache.invalidateLastCache(new MeasurementPath("root.db1.**.s2"));
    cacheEntry = dualKeyCache.get(new PartialPath("root.db1.d1"), "s2");
    Assert.assertNull(cacheEntry.getLastCacheContainer().getCachedLast());
  }
}
