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
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixPipePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CachedSchemaPatternMatcherTest {

  private static class MockedPipeRealtimeEvent extends PipeRealtimeEvent {

    public MockedPipeRealtimeEvent(
        EnrichedEvent event,
        TsFileEpoch tsFileEpoch,
        Map<String, String[]> device2Measurements,
        PipePattern pattern) {
      super(event, tsFileEpoch, device2Measurements, pattern);
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

  private CachedSchemaPatternMatcher matcher;
  private ExecutorService executorService;
  private List<PipeRealtimeDataRegionSource> extractors;

  @Before
  public void setUp() {
    matcher = new CachedSchemaPatternMatcher();
    executorService = Executors.newSingleThreadExecutor();
    extractors = new ArrayList<>();
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void testCachedMatcher() throws Exception {
    PipeRealtimeDataRegionSource dataRegionExtractor = new PipeRealtimeDataRegionFakeSource();
    dataRegionExtractor.customize(
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root");
              }
            }),
        new PipeTaskRuntimeConfiguration(new PipeTaskSourceRuntimeEnvironment("1", 1, 1, null)));
    extractors.add(dataRegionExtractor);

    int deviceExtractorNum = 10;
    int seriesExtractorNum = 10;
    for (int i = 0; i < deviceExtractorNum; i++) {
      PipeRealtimeDataRegionSource deviceExtractor = new PipeRealtimeDataRegionFakeSource();
      int finalI1 = i;
      deviceExtractor.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root." + finalI1);
                }
              }),
          new PipeTaskRuntimeConfiguration(new PipeTaskSourceRuntimeEnvironment("1", 1, 1, null)));
      extractors.add(deviceExtractor);
      for (int j = 0; j < seriesExtractorNum; j++) {
        PipeRealtimeDataRegionSource seriesExtractor = new PipeRealtimeDataRegionFakeSource();
        int finalI = i;
        int finalJ = j;
        seriesExtractor.customize(
            new PipeParameters(
                new HashMap<String, String>() {
                  {
                    put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root." + finalI + "." + finalJ);
                  }
                }),
            new PipeTaskRuntimeConfiguration(
                new PipeTaskSourceRuntimeEnvironment("1", 1, 1, null)));
        extractors.add(seriesExtractor);
      }
    }

    Future<?> future =
        executorService.submit(() -> extractors.forEach(extractor -> matcher.register(extractor)));

    int epochNum = 10000;
    int deviceNum = 1000;
    int seriesNum = 100;
    Map<String, String[]> deviceMap =
        IntStream.range(0, deviceNum)
            .mapToObj(String::valueOf)
            .collect(Collectors.toMap(s -> "root." + s, s -> new String[0]));
    String[] measurements =
        IntStream.range(0, seriesNum).mapToObj(String::valueOf).toArray(String[]::new);
    long totalTime = 0;
    for (int i = 0; i < epochNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        MockedPipeRealtimeEvent event =
            new MockedPipeRealtimeEvent(
                null, null, Collections.singletonMap("root." + i, measurements), null);
        final long startTime = System.currentTimeMillis();
        matcher.match(event).getLeft().forEach(extractor -> extractor.extract(event));
        totalTime += (System.currentTimeMillis() - startTime);
      }
      final MockedPipeRealtimeEvent event =
          new MockedPipeRealtimeEvent(null, null, deviceMap, null);
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

  public static class PipeRealtimeDataRegionFakeSource extends PipeRealtimeDataRegionSource {

    public PipeRealtimeDataRegionFakeSource() {
      pipePattern = new PrefixPipePattern(null);
    }

    @Override
    public Event supply() {
      return null;
    }

    @Override
    protected void doExtract(PipeRealtimeEvent event) {
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
                                .startsWith(getPatternString());
                  }
                } else {
                  match[0] =
                      match[0]
                          || (getPatternString().startsWith(k) || k.startsWith(getPatternString()));
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
