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
package org.apache.iotdb.backup.core.pipeline.out.source;

import org.apache.iotdb.backup.core.model.DeviceModel;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.model.TimeseriesModel;
import org.apache.iotdb.backup.core.pipeline.PipeSource;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/** @Author: LL @Description: @Date: create in 2022/11/24 17:11 */
public class OutTsfileDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(OutTsfileDataSource.class);

  private String name;

  private Scheduler scheduler;

  private ConcurrentHashMap<String, TsFileWriter> TSFILE_WRITER_MAP = new ConcurrentHashMap<>();

  private ExportPipelineService exportPipelineService;

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(s -> this.initOutputStream(s, TSFILE_WRITER_MAP))
            .flatMap(s -> exportPipelineService.countDeviceNum(s, totalSize))
            .flatMap(s -> exportPipelineService.parseToDeviceModel())
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(exportPipelineService::parseTimeseries)
            .flatMap(s -> registerDevice(s))
            .flatMap(exportPipelineService::parseToRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  try {
                    for (String key : TSFILE_WRITER_MAP.keySet()) {
                      TSFILE_WRITER_MAP.get(key).flushAllChunkGroups();
                      TSFILE_WRITER_MAP.get(key).close();
                    }
                    scheduler.dispose();
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                })
            .contextWrite(
                context -> {
                  context = context.put("totalSize", totalSize);
                  context = context.put("tsfileWriterMap", TSFILE_WRITER_MAP);
                  return context;
                });
  }

  /**
   * 初始化设备实体对应的outputStream
   *
   * @param str
   * @param tsFileWriterMap
   * @return
   */
  public Flux<String> initOutputStream(
      String str, ConcurrentHashMap<String, TsFileWriter> tsFileWriterMap) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          try {
            File file;
            String fileName;
            // TODO：根据虚拟存储组个数，时间分区确认有多少个tsfile文件
            // TODO：暂时先初始化一个
            fileName = exportModel.getFileFolder() + "one.tsfile";
            //                if (exportModel.getFileSinkStrategyEnum() ==
            // FileSinkStrategyEnum.EXTRA_CATALOG) {
            //                  fileName = exportModel.getFileFolder() + no;
            //                } else {
            //                  fileName = exportModel.getFileFolder() +
            // pair.getLeft().getDeviceName();
            //                }
            //                fileName = fileName + ".sql";
            file = new File(fileName);
            TsFileWriter tsFileWriter = new TsFileWriter(file);
            tsFileWriterMap.put("tsfile-0", tsFileWriter);
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          return Flux.just(str);
        });
  }

  public Flux<Pair<DeviceModel, List<TimeseriesModel>>> registerDevice(
      Pair<DeviceModel, List<TimeseriesModel>> pair) {
    return Flux.deferContextual(
        contextView -> {
          ConcurrentMap<String, TsFileWriter> tsfileWriterMap = contextView.get("tsfileWriterMap");
          TsFileWriter tsFileWriter = tsfileWriterMap.get("tsfile-0");
          List<TimeseriesModel> timeseriesModelList = pair.getRight();
          List<MeasurementSchema> measurementSchemaList =
              timeseriesModelList.stream()
                  .map(
                      model -> {
                        String columnName =
                            model.getName().substring(pair.getLeft().getDeviceName().length() + 1);
                        TSDataType type = model.getType();
                        MeasurementSchema measurementSchema =
                            new MeasurementSchema(columnName, type);
                        return measurementSchema;
                      })
                  .collect(Collectors.toList());

          Path path = new Path(pair.getLeft().getDeviceName());
          syncRegisterTimeseries(
              tsFileWriter, path, measurementSchemaList, pair.getLeft().isAligned());
          return Flux.just(pair);
        });
  }

  /**
   * tsfileWriter.registerTimeseries非线程安全方法，修改为线程安全的
   *
   * @param tsFileWriter
   * @param path
   * @param measurementSchemaList
   * @param isAligned
   */
  public void syncRegisterTimeseries(
      TsFileWriter tsFileWriter,
      Path path,
      List<MeasurementSchema> measurementSchemaList,
      boolean isAligned) {
    synchronized (tsFileWriter) {
      if (isAligned) {
        try {
          tsFileWriter.registerAlignedTimeseries(path, measurementSchemaList);
        } catch (WriteProcessException e) {
          e.printStackTrace();
        }
      } else {
        tsFileWriter.registerTimeseries(path, measurementSchemaList);
      }
    }
  }

  public OutTsfileDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public OutTsfileDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    scheduler = Schedulers.newParallel("pipeline-thread", this.parallelism);
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
