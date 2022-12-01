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
package org.apache.iotdb.backup.core;

import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.*;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.pipeline.out.channel.SimpleChannel;
import org.apache.iotdb.backup.core.pipeline.out.channel.StringFormatIncludeNullFieldChannel;
import org.apache.iotdb.backup.core.pipeline.out.channel.StringFormatWithoutNullFieldChannel;
import org.apache.iotdb.backup.core.pipeline.out.sink.*;
import org.apache.iotdb.backup.core.pipeline.out.source.*;

import com.alibaba.fastjson.JSON;
import reactor.core.Disposable;
import reactor.core.publisher.ParallelFlux;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ExportStarter implements Starter<ExportModel> {

  private List<PipeSink<TimeSeriesRowModel, TimeSeriesRowModel>> pipeSinkList = new ArrayList<>();
  private List<PipeSource> pipeSourceList = new ArrayList<>();

  private CommonPipeline pipeline;

  @Override
  public Disposable start(ExportModel exportModel) {

    pipeSinkList.clear();
    pipeSourceList.clear();

    PipelineBuilder builder = new PipelineBuilder();
    String fileFloder = exportModel.getFileFolder();
    if (!fileFloder.endsWith("/") && !fileFloder.endsWith("\\")) {
      fileFloder += File.separator;
    }
    exportModel.setFileFolder(fileFloder);
    File file = new File(fileFloder);
    if (!file.exists()) {
      file.mkdirs();
    }
    StringBuilder requestFilePath = new StringBuilder();
    requestFilePath.append(exportModel.getFileFolder()).append("REQUEST.json");
    String json = JSON.toJSONString(exportModel);
    try (OutputStreamWriter out = new FileWriter(requestFilePath.toString())) {
      out.write(json);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (exportModel.getNeedTimeseriesStructure()) {
      PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> outStructureFileSink =
          new OutStructureFileSink("structure sink");
      // pipeSinkList.add(outStructureFileSink);
      builder
          .source(new OutStructureSource("structure source"))
          .channel(new StringFormatIncludeNullFieldChannel("structure channel"))
          .sink(outStructureFileSink);
    }
    PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> pipeSink = generateSink(exportModel);
    pipeSinkList.add(pipeSink);
    PipeSource pipeSource = generateSource(exportModel);
    pipeSourceList.add(pipeSource);
    pipeline =
        builder
            .source(pipeSource)
            .channel(() -> generateChannel(exportModel))
            .sink(pipeSink)
            .build()
            .withContext(
                () -> {
                  PipelineContext<ExportModel> context = new PipelineContext<>();
                  context.setModel(exportModel);
                  return context;
                });
    return pipeline.start();
  }

  @Override
  public void shutDown() {
    this.pipeline.shutDown();
  }

  /**
   * 查看导出文件进度
   *
   * @return
   */
  @Override
  public Double[] rateOfProcess() {
    Double[] rateArray = new Double[2];
    pipeSinkList.forEach(
        pipesink -> {
          Double[] d = pipesink.rateOfProcess();
          rateArray[0] += d[0];
          rateArray[1] += d[1];
        });
    return rateArray;
  }

  /**
   * 查看文件导出行数
   *
   * @return
   */
  @Override
  public Long finishedRowNum() {
    AtomicLong result = new AtomicLong();
    pipeSinkList.forEach(
        pipesink -> {
          result.addAndGet(pipesink.finishedRowNum());
        });
    return result.get();
  }

  public PipeSource<
          String,
          TimeSeriesRowModel,
          Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>>
      generateSource(ExportModel exportModel) {
    PipeSource<
            String,
            TimeSeriesRowModel,
            Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>>
        pipeSource;
    switch (exportModel.getCompressEnum()) {
      case SQL:
        pipeSource = new OutSqlDataSource("sql source", exportModel.getParallelism());
        break;
      case CSV:
        pipeSource = new OutCsvDataSource("csv source", exportModel.getParallelism());
        break;
      case SNAPPY:
      case GZIP:
      case LZ4:
        pipeSource = new OutCompressDataSource("compress source", exportModel.getParallelism());
        break;
      case TSFILE:
        pipeSource = new OutTsfileDataSource("tsfile source", exportModel.getParallelism());
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + exportModel.getCompressEnum());
    }
    return pipeSource;
  }

  public PipeChannel<
          TimeSeriesRowModel,
          TimeSeriesRowModel,
          Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>>
      generateChannel(ExportModel exportModel) {
    PipeChannel<
            TimeSeriesRowModel,
            TimeSeriesRowModel,
            Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>>
        pipeChannel;
    switch (exportModel.getCompressEnum()) {
      case SQL:
        pipeChannel = new StringFormatWithoutNullFieldChannel("string format without null channel");
        break;
      case CSV:
      case SNAPPY:
      case GZIP:
      case LZ4:
        pipeChannel = new StringFormatIncludeNullFieldChannel("string format channel");
        break;
      case TSFILE:
        pipeChannel = new SimpleChannel("tsfile channel");
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + exportModel.getCompressEnum());
    }
    return pipeChannel;
  }

  public PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> generateSink(ExportModel exportModel) {
    PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> pipeSink;
    switch (exportModel.getCompressEnum()) {
      case SQL:
        pipeSink = new OutSqlFileSink("sql sink");
        break;
      case CSV:
        pipeSink = new OutCsvFileSink("csv sink");
        break;
      case SNAPPY:
      case GZIP:
      case LZ4:
        pipeSink = new OutCompressFileSink("compress sink");
        break;
      case TSFILE:
        pipeSink = new OutTsfileDataSink("tsfile sink");
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + exportModel.getCompressEnum());
    }
    return pipeSink;
  }
}
