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

package org.apache.iotdb.db.pipe.collector;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.collector.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskCollectorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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
  private final TConsensusGroupId dataRegionId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

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
    PipeRealtimeDataRegionCollector databaseCollector = new PipeRealtimeDataRegionFakeCollector();
    databaseCollector.customize(
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, "root");
              }
            }),
        new PipeTaskRuntimeConfiguration(
            new PipeTaskCollectorRuntimeEnvironment("1", 1, dataRegionId, null)));
    collectorList.add(databaseCollector);

    int deviceCollectorNum = 10;
    int seriesCollectorNum = 10;
    for (int i = 0; i < deviceCollectorNum; i++) {
      PipeRealtimeDataRegionCollector deviceCollector = new PipeRealtimeDataRegionFakeCollector();
      int finalI1 = i;
      deviceCollector.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, "root." + finalI1);
                }
              }),
          new PipeTaskRuntimeConfiguration(
              new PipeTaskCollectorRuntimeEnvironment("1", 1, dataRegionId, null)));
      collectorList.add(deviceCollector);
      for (int j = 0; j < seriesCollectorNum; j++) {
        PipeRealtimeDataRegionCollector seriesCollector = new PipeRealtimeDataRegionFakeCollector();
        int finalI = i;
        int finalJ = j;
        seriesCollector.customize(
            new PipeParameters(
                new HashMap<String, String>() {
                  {
                    put(
                        PipeCollectorConstant.COLLECTOR_PATTERN_KEY,
                        "root." + finalI + "." + finalJ);
                  }
                }),
            new PipeTaskRuntimeConfiguration(
                new PipeTaskCollectorRuntimeEnvironment("1", 1, dataRegionId, null)));
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
                null, null, Collections.singletonMap("root." + i, measurements), "root");
        long startTime = System.currentTimeMillis();
        matcher.match(event).forEach(collector -> collector.collect(event));
        totalTime += (System.currentTimeMillis() - startTime);
      }
      PipeRealtimeCollectEvent event = new PipeRealtimeCollectEvent(null, null, deviceMap, "root");
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

    public PipeRealtimeDataRegionFakeCollector() {}

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
