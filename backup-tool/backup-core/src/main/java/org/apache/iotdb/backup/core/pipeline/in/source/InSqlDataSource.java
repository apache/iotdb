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
package org.apache.iotdb.backup.core.pipeline.in.source;

import org.apache.iotdb.backup.core.pipeline.PipeSource;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.backup.core.service.ImportPipelineService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** @Author: LL @Description: @Date: create in 2022/6/29 9:59 */
public class InSqlDataSource
    extends PipeSource<String, String, Function<ParallelFlux<String>, ParallelFlux<String>>> {

  private static final Logger log = LoggerFactory.getLogger(InSqlDataSource.class);

  private String name;

  private ImportPipelineService importPipelineService;

  private Scheduler scheduler;

  private static final String CATALOG_SQL = "CATALOG_SQL.CATALOG";

  private ConcurrentHashMap<String, List<InputStream>> COMPRESS_MAP = new ConcurrentHashMap<>();

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<String>> doExecute() {
    return flux -> {
      return flux.flatMap(
              s -> {
                return Flux.deferContextual(
                    contextView -> {
                      PipelineContext<ImportModel> context = contextView.get("pipelineContext");
                      ImportModel importModel = context.getModel();
                      FilenameFilter fileFilter =
                          new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                              if (CATALOG_SQL.equals(name)) {
                                return false;
                              }
                              if (!name.toLowerCase().endsWith(".sql")) {
                                return false;
                              }
                              return true;
                            }
                          };
                      totalSize[0] =
                          importPipelineService.getFileArray(
                                  fileFilter, importModel.getFileFolder())
                              .length;
                      return importPipelineService.parseFluxFileName(fileFilter, COMPRESS_MAP);
                    });
              })
          .parallel(parallelism)
          .runOn(scheduler)
          .flatMap(this::parseFluxSqlString) // 通过inputstream 读取sqlString
          .transform(doNext())
          .sequential()
          .doFinally(
              signalType -> {
                for (String key : COMPRESS_MAP.keySet()) {
                  COMPRESS_MAP
                      .get(key)
                      .forEach(
                          inputStream -> {
                            if (inputStream != null) {
                              try {
                                inputStream.close();
                              } catch (IOException e) {
                              }
                            }
                          });
                }
                scheduler.dispose();
              })
          .contextWrite(
              context -> {
                return context.put("totalSize", totalSize);
              });
    };
  }

  /**
   * 读取txt文本中的sql，以每一条sql为元素构建flux
   *
   * @param inputStream
   * @return
   */
  public Flux<String> parseFluxSqlString(InputStream inputStream) {
    return Flux.deferContextual(
        contextView -> {
          String version = contextView.get("VERSION");
          return Flux.create(
              fluxSink -> {
                try {
                  StringBuilder sql = new StringBuilder();
                  int i;
                  while ((i = inputStream.read()) != -1) {
                    if (i == '\n') {
                      String s = sql.toString().replace(";", "");
                      String s1 = s.substring(0, s.indexOf("root"));
                      String s2 = s.substring(s.indexOf("root"), s.indexOf("("));
                      s1 = s1 + ExportPipelineService.formatPath(s2, version);
                      String needFormat = s.substring(s.indexOf("(") + 1, s.indexOf(")"));
                      String s3 = s.substring(s.indexOf(")"), s.length());
                      StringBuilder result = new StringBuilder();
                      result
                          .append(s1)
                          .append("(")
                          .append(ExportPipelineService.formatPath(needFormat, version))
                          .append(s3);
                      sql.delete(0, sql.length());
                      fluxSink.next(result.toString());
                    } else if (i == '\r') {

                    } else {
                      sql = sql.append((char) i);
                    }
                  }
                  fluxSink.next("finish");
                  fluxSink.complete();
                } catch (IOException e) {
                  fluxSink.error(e);
                } finally {
                  try {
                    if (inputStream != null) {
                      inputStream.close();
                    }
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                }
              });
        });
  }

  public InSqlDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public InSqlDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    this.scheduler = Schedulers.newParallel("pipeline-thread", this.parallelism);
    if (this.importPipelineService == null) {
      this.importPipelineService = ImportPipelineService.importPipelineService();
    }
  }
}
