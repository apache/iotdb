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
package org.apache.iotdb.backup.core.pipeline.in.sink;

import org.apache.iotdb.backup.core.model.FieldCopy;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSink;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InStructureFileSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

  private static final Logger log = LoggerFactory.getLogger(InStructureFileSink.class);

  private String name;

  private AtomicInteger finishedFileNum = new AtomicInteger();

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
                        // createAligned
                        // 有bug,只能先一个个的创建了，描述，如果一个序列被截断为两分数据，比如：8个时间序列，1个先执行，后7个在执行同样的的方法报异常
                        // Timeseries under this entity is not aligned, please use createTimeseries
                        // or change entity
                        // api接口还是有问题，时间序列对齐的方法改为执行insert语句
                        .buffer(1000, 1000)
                        .flatMap(
                            list -> {
                              return Flux.deferContextual(
                                  contextView -> {
                                    PipelineContext<ImportModel> context =
                                        contextView.get("pipelineContext");
                                    ImportModel importModel = context.getModel();
                                    String version = contextView.get("VERSION");
                                    Session session = importModel.getSession();

                                    List<String> paths = new ArrayList<>();
                                    List<TSDataType> dataTypes = new ArrayList<>();
                                    List<TSEncoding> encodings = new ArrayList<>();
                                    List<CompressionType> compressors = new ArrayList<>();
                                    // 这个属性不知道是干啥的
                                    List<Map<String, String>> propsList = null;
                                    List<Map<String, String>> tagsList = new ArrayList<>();
                                    List<Map<String, String>> attributesList = new ArrayList<>();
                                    // TODO: 这里有bug，measurementAliasList 中放入null报空指针，放入“”报重复数据
                                    // 暂时放的是measurementList
                                    List<String> measurementAliasList = new ArrayList<>();

                                    // 按照设备实体路径，把数据分组
                                    Map<String, List<TimeSeriesRowModel>> groupMap =
                                        list.stream()
                                            .collect(
                                                Collectors.toMap(
                                                    s -> s.getDeviceModel().getDeviceName(),
                                                    p -> {
                                                      List<TimeSeriesRowModel> row =
                                                          new ArrayList<>();
                                                      row.add(p);
                                                      return row;
                                                    },
                                                    (v1, v2) -> {
                                                      v1.addAll(v2);
                                                      return v1;
                                                    }));

                                    for (String key : groupMap.keySet()) {
                                      List<TimeSeriesRowModel> rowModel = groupMap.get(key);

                                      rowModel.stream()
                                          .forEach(
                                              s -> {
                                                s.getIFieldList().stream()
                                                    .forEach(
                                                        v -> {
                                                          FieldCopy field = v.getField();
                                                          switch (v.getColumnName()) {
                                                            case ("timeseries"):
                                                              paths.add(field.getStringValue());
                                                              break;
                                                            case ("alias"):
                                                              if (field == null) {
                                                                measurementAliasList.add(null);
                                                              } else {
                                                                measurementAliasList.add(
                                                                    field.getStringValue());
                                                              }
                                                              break;
                                                            case ("storage group"):
                                                              break;
                                                            case ("dataType"):
                                                              dataTypes.add(
                                                                  TSDataType.valueOf(
                                                                      field.getStringValue()));
                                                              break;
                                                            case ("encoding"):
                                                              encodings.add(
                                                                  TSEncoding.valueOf(
                                                                      field.getStringValue()));
                                                              break;
                                                            case ("compression"):
                                                              compressors.add(
                                                                  CompressionType.valueOf(
                                                                      field.getStringValue()));
                                                              break;
                                                            case ("tags"):
                                                              HashMap<String, String> map1 =
                                                                  new HashMap<>();
                                                              if (field == null) {
                                                                tagsList.add(map1);
                                                              } else {
                                                                String value =
                                                                    field
                                                                        .getStringValue()
                                                                        .replaceAll("\"", "");
                                                                String[] arr =
                                                                    value
                                                                        .substring(
                                                                            1, value.length() - 1)
                                                                        .split(",");
                                                                for (int i = 0;
                                                                    i < arr.length;
                                                                    i++) {
                                                                  String[] kv = arr[i].split(":");
                                                                  map1.put(kv[0], kv[1]);
                                                                }
                                                                tagsList.add(map1);
                                                              }
                                                              break;
                                                            case ("attributes"):
                                                              HashMap<String, String> map2 =
                                                                  new HashMap<>();
                                                              if (field == null) {
                                                                attributesList.add(map2);
                                                              } else {
                                                                String value =
                                                                    field
                                                                        .getStringValue()
                                                                        .replaceAll("\"", "");
                                                                String[] arr =
                                                                    value
                                                                        .substring(
                                                                            1, value.length() - 1)
                                                                        .split(",");
                                                                for (int i = 0;
                                                                    i < arr.length;
                                                                    i++) {
                                                                  String[] kv = arr[i].split(":");
                                                                  map2.put(kv[0], kv[1]);
                                                                }
                                                                attributesList.add(map2);
                                                              }
                                                          }
                                                        });
                                              });
                                      // 设备对齐和不对齐，导入方法不一样
                                      try {
                                        List<String> measurmentlist =
                                            paths.stream()
                                                .map(
                                                    s -> {
                                                      return s.substring(
                                                          s.lastIndexOf(".") + 1, s.length());
                                                    })
                                                .collect(Collectors.toList());
                                        if (rowModel.get(0).getDeviceModel().isAligned()) {
                                          StringBuilder b1 = new StringBuilder();
                                          StringBuilder b2 = new StringBuilder();
                                          b1.append("CREATE ALIGNED TIMESERIES ")
                                              .append(
                                                  rowModel.get(0).getDeviceModel().getDeviceName())
                                              .append("(");

                                          for (int i = 0; i < measurmentlist.size(); i++) {
                                            String measurement = measurmentlist.get(i);
                                            TSDataType dataType = dataTypes.get(i);
                                            TSEncoding encoding = encodings.get(i);
                                            CompressionType compressionType = compressors.get(i);
                                            if (b2.length() != 0) {
                                              b2.append(",");
                                            }
                                            b2.append(
                                                    ExportPipelineService.formatMeasurement(
                                                        measurement, version))
                                                .append(" ")
                                                .append(dataType.toString())
                                                .append(" ")
                                                .append("encoding=")
                                                .append(encoding.toString())
                                                .append(" ")
                                                .append("compressor=")
                                                .append(compressionType.toString());
                                          }
                                          b1.append(b2).append(")");
                                          session.executeNonQueryStatement(b1.toString());
                                        } else {
                                          // 如果有alias使用原生的，没有就使用measurement名称
                                          for (int i = 0; i < measurementAliasList.size(); i++) {
                                            String alias = measurementAliasList.get(i);
                                            if (alias == null) {
                                              alias = measurmentlist.get(i);
                                              measurementAliasList.set(i, alias);
                                            }
                                          }
                                          session.createMultiTimeseries(
                                              paths,
                                              dataTypes,
                                              encodings,
                                              compressors,
                                              propsList,
                                              tagsList,
                                              attributesList,
                                              measurementAliasList);
                                        }
                                      } catch (IoTDBConnectionException
                                          | StatementExecutionException e) {
                                        if (e instanceof BatchExecutionException) {
                                          if (e.getMessage().indexOf("PathAlreadyExistException")
                                              > -1) {
                                            log.debug(e.getMessage());
                                          }
                                        } else if (e.getMessage().indexOf("already exist") > -1) {
                                          log.debug(e.getMessage());
                                        } else {
                                          log.error("出现异常", e);
                                        }
                                      }
                                      paths.clear();
                                      dataTypes.clear();
                                      encodings.clear();
                                      compressors.clear();
                                      tagsList.clear();
                                      attributesList.clear();
                                      measurementAliasList.clear();
                                    }
                                    finishedRowNum.addAndGet(list.size());
                                    return Flux.fromIterable(list);
                                  });
                            })
                        .doOnComplete(
                            () -> {
                              finishedFileNum.incrementAndGet();
                            }));
  }

  @Override
  public Double[] rateOfProcess() {
    log.info("已经导入文件：{}", finishedFileNum);
    log.info("总文件数：{}", 1);
    Double[] rateDouble = new Double[2];
    rateDouble[0] = finishedFileNum.doubleValue();
    rateDouble[1] = 1d;
    return rateDouble;
  }

  @Override
  public Long finishedRowNum() {
    return finishedRowNum.get();
  }

  public InStructureFileSink(String name) {
    this.name = name;
  }
}
