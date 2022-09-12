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

import org.apache.iotdb.backup.core.pipeline.PipeSink;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class InSqlFileSink extends PipeSink<String, String> {

  private static final Logger log = LoggerFactory.getLogger(InSqlFileSink.class);

  private String name;

  private AtomicInteger finishedFileNum = new AtomicInteger();

  private Integer[] totalFileNum = new Integer[1];

  private AtomicLong finishedRowNum = new AtomicLong();

  @Override
  public Function<ParallelFlux<String>, ParallelFlux<String>> doExecute() {
    return sink ->
        sink.flatMap(
            s -> {
              return Flux.deferContextual(
                  contextView -> {
                    PipelineContext<ImportModel> pcontext = contextView.get("pipelineContext");
                    totalFileNum = contextView.get("totalSize");
                    ImportModel importModel = pcontext.getModel();
                    Session session = importModel.getSession();
                    try {
                      if (s.startsWith("finish")) {
                        finishedFileNum.incrementAndGet();
                      } else {
                        session.executeNonQueryStatement(s);
                        finishedRowNum.incrementAndGet();
                      }
                    } catch (StatementExecutionException | IoTDBConnectionException e) {
                      log.error("异常信息:", e);
                    }
                    return ParallelFlux.from(Flux.just(s));
                  });
            });
  }

  @Override
  public Double[] rateOfProcess() {
    log.info("已经导出文件：{}", finishedFileNum);
    log.info("总文件数：{}", totalFileNum[0]);
    Double[] rateDouble = new Double[2];
    rateDouble[0] = finishedFileNum.doubleValue();
    rateDouble[1] = Double.parseDouble(String.valueOf(totalFileNum[0]));
    return rateDouble;
  }

  @Override
  public Long finishedRowNum() {
    return finishedRowNum.get();
  }

  public InSqlFileSink(String name) {
    this.name = name;
  }
}
