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

package org.apache.iotdb.db.pipe.core.collector;

import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

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

  private CachedSchemaPatternMatcher matcher;
  private ExecutorService executorService;
  private List<PipeRealtimeDataRegionCollector> collectorList;

  @Before
  public void setUp() {
    matcher = new CachedSchemaPatternMatcher();
    executorService = Executors.newSingleThreadExecutor();
    collectorList = new ArrayList<>();
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void testCachedMatcher() throws Exception {
    PipeRealtimeDataRegionCollector databaseCollector =
        new PipeRealtimeDataRegionFakeCollector(null);
    databaseCollector.customize(
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, "root");
                put(PipeCollectorConstant.DATA_REGION_KEY, "1");
              }
            }),
        null);
    collectorList.add(databaseCollector);

    int deviceCollectorNum = 10;
    int seriesCollectorNum = 10;
    for (int i = 0; i < deviceCollectorNum; i++) {
      PipeRealtimeDataRegionCollector deviceCollector =
          new PipeRealtimeDataRegionFakeCollector(null);
      int finalI1 = i;
      deviceCollector.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, "root." + finalI1);
                  put(PipeCollectorConstant.DATA_REGION_KEY, "1");
                }
              }),
          null);
      collectorList.add(deviceCollector);
      for (int j = 0; j < seriesCollectorNum; j++) {
        PipeRealtimeDataRegionCollector seriesCollector =
            new PipeRealtimeDataRegionFakeCollector(null);
        int finalI = i;
        int finalJ = j;
        seriesCollector.customize(
            new PipeParameters(
                new HashMap<String, String>() {
                  {
                    put(
                        PipeCollectorConstant.COLLECTOR_PATTERN_KEY,
                        "root." + finalI + "." + finalJ);
                    put(PipeCollectorConstant.DATA_REGION_KEY, "1");
                  }
                }),
            null);
        collectorList.add(seriesCollector);
      }
    }

    Future<?> future =
        executorService.submit(
            () -> collectorList.forEach(collector -> matcher.register(collector)));

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
        PipeRealtimeCollectEvent event =
            new PipeRealtimeCollectEvent(
                null, null, Collections.singletonMap("root." + i, measurements));
        long startTime = System.currentTimeMillis();
        matcher.match(event).forEach(collector -> collector.collect(event));
        totalTime += (System.currentTimeMillis() - startTime);
      }
      PipeRealtimeCollectEvent event = new PipeRealtimeCollectEvent(null, null, deviceMap);
      long startTime = System.currentTimeMillis();
      matcher.match(event).forEach(collector -> collector.collect(event));
      totalTime += (System.currentTimeMillis() - startTime);
    }
    System.out.println("matcher.getRegisterCount() = " + matcher.getRegisterCount());
    System.out.println("totalTime = " + totalTime);
    System.out.println(
        "device match per second = "
            + ((double) (epochNum * (deviceNum + 1)) / (double) (totalTime) * 1000.0));

    future.get();
  }

  public static class PipeRealtimeDataRegionFakeCollector extends PipeRealtimeDataRegionCollector {

    public PipeRealtimeDataRegionFakeCollector(PipeTaskMeta pipeTaskMeta) {
      super(pipeTaskMeta);
    }

    @Override
    public Event supply() {
      return null;
    }

    @Override
    public void collect(PipeRealtimeCollectEvent event) {
      final boolean[] match = {false};
      event
          .getSchemaInfo()
          .forEach(
              (k, v) -> {
                if (v.length > 0) {
                  for (String s : v) {
                    match[0] =
                        match[0]
                            || (k + TsFileConstant.PATH_SEPARATOR + s).startsWith(getPattern());
                  }
                } else {
                  match[0] = match[0] || (getPattern().startsWith(k) || k.startsWith(getPattern()));
                }
              });
      Assert.assertTrue(match[0]);
    }
  }
}
