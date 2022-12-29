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
package org.apache.iotdb.backup.core.pipeline.out.sink;

import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSink;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OutCompressFileSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

  private static final Logger log = LoggerFactory.getLogger(OutCompressFileSink.class);

  private String name;

  private ExportPipelineService exportPipelineService;

  private AtomicInteger finishedFileNum = new AtomicInteger();

  private int totalFileNum;
  // 数据条数
  private AtomicLong finishedRowNum = new AtomicLong();

  @Override
  public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
    return sink ->
        sink.transformGroups(
                (Function<
                        GroupedFlux<Integer, TimeSeriesRowModel>,
                        Publisher<? extends TimeSeriesRowModel>>)
                    integerTimeSeriesRowModelGroupedFlux ->
                        integerTimeSeriesRowModelGroupedFlux
                            .buffer(15000, 15000)
                            .flatMap(
                                allList -> {
                                  return Flux.deferContextual(
                                      contextView -> {
                                        ConcurrentHashMap<String, OutputStream> outputStreamMap =
                                            contextView.get("outputStreamMap");
                                        Integer[] totalSize = contextView.get("totalSize");
                                        totalFileNum = totalSize[0];
                                        PipelineContext<ExportModel> pcontext =
                                            contextView.get("pipelineContext");
                                        ExportModel exportModel = pcontext.getModel();
                                        Map<String, List<TimeSeriesRowModel>> groupMap =
                                            allList.stream()
                                                .collect(
                                                    Collectors.toMap(
                                                        k -> k.getDeviceModel().getDeviceName(),
                                                        p -> {
                                                          List<TimeSeriesRowModel> result =
                                                              new ArrayList();
                                                          result.add(p);
                                                          return result;
                                                        },
                                                        (o, n) -> {
                                                          o.addAll(n);
                                                          return o;
                                                        }));
                                        for (String groupKey : groupMap.keySet()) {
                                          if (groupKey.startsWith("finish")) {
                                            finishedFileNum.incrementAndGet();
                                            continue;
                                          }
                                          // TODO: 压缩文本，outstream写入 exportModel.getcompressEnum
                                          List<TimeSeriesRowModel> groupList =
                                              groupMap.get(groupKey);
                                          OutputStream sout = outputStreamMap.get(groupKey);
                                          exportPipelineService.compressBlock(
                                              groupList, sout, exportModel);
                                          finishedRowNum.addAndGet(groupList.size());
                                        }
                                        return Flux.fromIterable(allList);
                                      });
                                }))
            .flatMap(
                timeSeriesRowModel -> {
                  return Flux.deferContextual(
                      contextView -> {
                        String deviceName = timeSeriesRowModel.getDeviceModel().getDeviceName();
                        if (deviceName.startsWith("finish")) {
                          ConcurrentHashMap<String, OutputStream> outputStreamMap =
                              contextView.get("outputStreamMap");
                          deviceName =
                              deviceName.substring(
                                  deviceName.indexOf(",") + 1, deviceName.length());
                          OutputStream outputStream = outputStreamMap.get(deviceName);
                          outputStreamMap.remove(deviceName);
                          try {
                            outputStream.flush();
                            outputStream.close();
                          } catch (IOException e) {
                            log.error("outputStream 关闭异常：", e);
                          }
                        }
                        return Flux.just(timeSeriesRowModel);
                      });
                });
  }

  @Override
  public Double[] rateOfProcess() {
    log.info("已经导出文件：{}", finishedFileNum);
    log.info("总文件数：{}", totalFileNum);
    Double[] rateDouble = new Double[2];
    rateDouble[0] = finishedFileNum.doubleValue();
    rateDouble[1] = Double.parseDouble(String.valueOf(totalFileNum));
    return rateDouble;
  }

  @Override
  public Long finishedRowNum() {
    return finishedRowNum.get();
  }

  public OutCompressFileSink(String name) {
    this.name = name;
    if (this.exportPipelineService == null) {
      // TODO： 是否需要单例
      this.exportPipelineService = ExportPipelineService.exportPipelineService();
    }
  }
}
