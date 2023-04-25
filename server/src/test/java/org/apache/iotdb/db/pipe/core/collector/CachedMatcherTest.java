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

import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollectorManager;
import org.apache.iotdb.db.pipe.core.collector.realtime.matcher.CachedMatcher;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CachedMatcherTest {
  private PipeRealtimeCollectorManager manager;
  private CachedMatcher matcher;
  private ExecutorService executorService;
  private List<PipeRealtimeCollector> collectorList;

  @Before
  public void setUp() {
    manager = new PipeRealtimeCollectorManager();
    matcher = new CachedMatcher();
    executorService = Executors.newSingleThreadExecutor();
    collectorList = new ArrayList<>();
  }

  @Test
  public void testCachedMatcher() throws ExecutionException, InterruptedException {
    PipeRealtimeCollector databaseCollector = new PipeRealtimeFakeCollector("root", "1", manager);
    collectorList.add(databaseCollector);

    int deviceCollectorNum = 10;
    int seriesCollectorNum = 10;
    for (int i = 0; i < deviceCollectorNum; i++) {
      PipeRealtimeCollector deviceCollector =
          new PipeRealtimeFakeCollector("root." + i, "1", manager);
      collectorList.add(deviceCollector);
      for (int j = 0; j < seriesCollectorNum; j++) {
        PipeRealtimeCollector seriesCollector =
            new PipeRealtimeFakeCollector("root." + i + "." + j, "1", manager);
        collectorList.add(seriesCollector);
      }
    }

    Future<?> future =
        executorService.submit(
            () -> {
              collectorList.forEach(collector -> matcher.register(collector));
              //                Thread.sleep(20000);
              //                collectorList.forEach(collector -> matcher.deregister(collector));
            });

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
                null, Collections.singletonMap("root." + i, measurements), null);
        long startTime = System.currentTimeMillis();
        Set<PipeRealtimeCollector> collectorSet = matcher.match(event.getSchemaInfo());
        totalTime += (System.currentTimeMillis() - startTime);
        collectorSet.forEach(collector -> collector.collectEvent(event));
      }
      PipeRealtimeCollectEvent event = new PipeRealtimeCollectEvent(null, deviceMap, null);
      long startTime = System.currentTimeMillis();
      Set<PipeRealtimeCollector> collectorSet = matcher.match(event.getSchemaInfo());
      totalTime += (System.currentTimeMillis() - startTime);
      collectorSet.forEach(collector -> collector.collectEvent(event));
    }
    System.out.println("matcher.getRegisterCount() = " + matcher.getRegisterCount());
    System.out.println("totalTime = " + totalTime);
    System.out.println(
        "device match per second = "
            + ((double) (epochNum * (deviceNum + 1)) / (double) (totalTime) * 1000.0));

    future.get();
  }

  public static class PipeRealtimeFakeCollector extends PipeRealtimeCollector {

    public PipeRealtimeFakeCollector(
        String pattern, String dataRegionId, PipeRealtimeCollectorManager manager) {
      super(pattern, dataRegionId, manager);
    }

    @Override
    public Event supply() {
      return null;
    }

    @Override
    public void collectEvent(PipeRealtimeCollectEvent event) {
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
