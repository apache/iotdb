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

package org.apache.iotdb.db.pipe.pattern;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CachedSchemaPatternMatcherTest {

  private static class MockedPipeRealtimeEvent extends PipeRealtimeEvent {

    public MockedPipeRealtimeEvent(
        final EnrichedEvent event,
        final TsFileEpoch tsFileEpoch,
        final Map<IDeviceID, String[]> device2Measurements) {
      super(event, tsFileEpoch, device2Measurements);
    }

    @Override
    public boolean shouldParseTime() {
      return false;
    }

    @Override
    public boolean shouldParsePattern() {
      return false;
    }
  }

  private static class CountingCachedSchemaPatternMatcher extends CachedSchemaPatternMatcher {

    private int tableMatchCount;

    @Override
    protected void matchTableModelEvent(
        final String databaseName,
        final String tableName,
        final Set<PipeRealtimeDataRegionSource> matchedSources) {
      ++tableMatchCount;
      // Simulate a successful table-level match so this test focuses on match orchestration.
      matchedSources.addAll(sources);
    }

    private int getTableMatchCount() {
      return tableMatchCount;
    }
  }

  private CachedSchemaPatternMatcher matcher;
  private ExecutorService executorService;
  private List<PipeRealtimeDataRegionSource> extractors;
  private int dataNodeId;

  @Before
  public void setUp() {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    matcher = new CachedSchemaPatternMatcher();
    executorService = Executors.newSingleThreadExecutor();
    extractors = new ArrayList<>();
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
  }

  @Test
  public void testCachedMatcher() throws Exception {
    final PipeRealtimeDataRegionSource dataRegionExtractor = new PipeRealtimeDataRegionFakeSource();
    dataRegionExtractor.customize(
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root");
              }
            }),
        new PipeTaskRuntimeConfiguration(new PipeTaskSourceRuntimeEnvironment("1", 1, 1, null)));
    extractors.add(dataRegionExtractor);

    final int deviceExtractorNum = 10;
    final int seriesExtractorNum = 10;
    for (int i = 0; i < deviceExtractorNum; i++) {
      final PipeRealtimeDataRegionSource deviceExtractor = new PipeRealtimeDataRegionFakeSource();
      int finalI1 = i;
      deviceExtractor.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root.db" + finalI1);
                }
              }),
          new PipeTaskRuntimeConfiguration(new PipeTaskSourceRuntimeEnvironment("1", 1, 1, null)));
      extractors.add(deviceExtractor);
      for (int j = 0; j < seriesExtractorNum; j++) {
        final PipeRealtimeDataRegionSource seriesExtractor = new PipeRealtimeDataRegionFakeSource();
        int finalI = i;
        int finalJ = j;
        seriesExtractor.customize(
            new PipeParameters(
                new HashMap<String, String>() {
                  {
                    put(
                        PipeSourceConstant.EXTRACTOR_PATTERN_KEY,
                        "root.db" + finalI + ".s" + finalJ);
                  }
                }),
            new PipeTaskRuntimeConfiguration(
                new PipeTaskSourceRuntimeEnvironment("1", 1, 1, null)));
        extractors.add(seriesExtractor);
      }
    }

    final Future<?> future =
        executorService.submit(() -> extractors.forEach(extractor -> matcher.register(extractor)));

    final int epochNum = 10000;
    final int deviceNum = 1000;
    final int seriesNum = 100;
    final Map<IDeviceID, String[]> deviceMap =
        IntStream.range(0, deviceNum)
            .mapToObj(String::valueOf)
            .collect(
                Collectors.toMap(s -> new StringArrayDeviceID("root.db" + s), s -> new String[0]));
    final String[] measurements =
        IntStream.range(0, seriesNum).mapToObj(num -> "s" + num).toArray(String[]::new);
    long totalTime = 0;
    for (int i = 0; i < epochNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        final MockedPipeRealtimeEvent event =
            new MockedPipeRealtimeEvent(
                null,
                null,
                Collections.singletonMap(new StringArrayDeviceID("root.db" + i), measurements));
        final long startTime = System.currentTimeMillis();
        matcher.match(event).getLeft().forEach(extractor -> extractor.extract(event));
        totalTime += (System.currentTimeMillis() - startTime);
      }
      final MockedPipeRealtimeEvent event = new MockedPipeRealtimeEvent(null, null, deviceMap);
      final long startTime = System.currentTimeMillis();
      matcher.match(event).getLeft().forEach(extractor -> extractor.extract(event));
      totalTime += (System.currentTimeMillis() - startTime);
    }
    System.out.println("matcher.getRegisterCount() = " + matcher.getRegisterCount());
    System.out.println("totalTime = " + totalTime);
    System.out.println(
        "device match per second = "
            + ((double) (epochNum * (deviceNum + 1)) / (double) (totalTime) * 1000.0));

    future.get();
  }

  @Test
  public void testTableModelMatchesEachTableOncePerEvent() throws Exception {
    final CountingCachedSchemaPatternMatcher countingMatcher =
        new CountingCachedSchemaPatternMatcher();
    final PipeRealtimeDataRegionSource source = new PipeRealtimeDataRegionFakeSource();
    countingMatcher.register(source);

    final PipeInsertionEvent insertionEvent = Mockito.mock(PipeInsertionEvent.class);
    Mockito.when(insertionEvent.getTableModelDatabaseName()).thenReturn("db");
    final Map<IDeviceID, String[]> schemaInfo = new LinkedHashMap<>();
    schemaInfo.put(new StringArrayDeviceID("table1", "tag1"), new String[0]);
    schemaInfo.put(new StringArrayDeviceID("table1", "tag2"), new String[0]);

    Assert.assertTrue(
        countingMatcher
            .match(new MockedPipeRealtimeEvent(insertionEvent, null, schemaInfo))
            .getLeft()
            .contains(source));
    Assert.assertEquals(1, countingMatcher.getTableMatchCount());
  }

  @Test
  public void testMultiTableTsFileCollectsAllTableNamesAfterAllSourcesMatched() throws Exception {
    final CountingCachedSchemaPatternMatcher countingMatcher =
        new CountingCachedSchemaPatternMatcher();
    final PipeRealtimeDataRegionSource source = new PipeRealtimeDataRegionFakeSource();
    countingMatcher.register(source);

    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        Mockito.mock(PipeTsFileInsertionEvent.class);
    Mockito.when(tsFileInsertionEvent.isTableModelEvent()).thenReturn(true);
    Mockito.when(tsFileInsertionEvent.getTableModelDatabaseName()).thenReturn("db");
    final Map<IDeviceID, String[]> schemaInfo = new LinkedHashMap<>();
    schemaInfo.put(new StringArrayDeviceID("table1", "tag1"), new String[0]);
    schemaInfo.put(new StringArrayDeviceID("table2", "tag2"), new String[0]);

    countingMatcher.match(new MockedPipeRealtimeEvent(tsFileInsertionEvent, null, schemaInfo));

    final ArgumentCaptor<Set<String>> tableNamesCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(tsFileInsertionEvent).setTableNames(tableNamesCaptor.capture());
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("table1", "table2")), tableNamesCaptor.getValue());
    Assert.assertEquals(1, countingMatcher.getTableMatchCount());
  }

  public static class PipeRealtimeDataRegionFakeSource extends PipeRealtimeDataRegionSource {

    public PipeRealtimeDataRegionFakeSource() {
      treePattern = new PrefixTreePattern(null);
    }

    @Override
    public Event supply() {
      return null;
    }

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      final boolean[] match = {false};
      event
          .getSchemaInfo()
          .forEach(
              (k, v) -> {
                if (v.length > 0) {
                  for (String s : v) {
                    match[0] =
                        match[0]
                            || (k + TsFileConstant.PATH_SEPARATOR + s)
                                .startsWith(getTreePattern().getPattern());
                  }
                } else {
                  match[0] =
                      match[0]
                          || (getTreePattern().getPattern().startsWith(k.toString())
                              || k.toString().startsWith(getTreePattern().getPattern()));
                }
              });
      Assert.assertTrue(match[0]);
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return true;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return true;
    }
  }
}
