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
package org.apache.iotdb.backup.core.pipeline;

import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.IECommonModel;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CommonPipeline implements Pipeline {

  private static final Logger log = LoggerFactory.getLogger(CommonPipeline.class);

  private List<Component> componentChain;

  private PipelineContext<IECommonModel> pipelineContext = new PipelineContext<>();

  private List<Disposable> disposableList = new ArrayList<>();

  /**
   * 某些情况，需要等到任务执行结束，参考代码 while (!disposable.isDisposed()){ Thread.sleep(1000); }
   *
   * @return
   */
  @Override
  public Disposable start() {
    Iterator<Component> it = componentChain.iterator();
    if (it.hasNext()) {
      return syncRun(it, null);
    }
    return null;
  }

  private Disposable syncRun(Iterator<Component> it, Disposable disposable) {
    Component component = it.next();
    Flux pipelineFlux = Flux.just("------------------pipeline start------------- \r\n");
    if (disposable != null) {
      Disposable finalDisposable = disposable;
      pipelineFlux =
          pipelineFlux
              .publishOn(Schedulers.newSingle("single"))
              .flatMap(
                  s -> {
                    while (!finalDisposable.isDisposed()) {
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                        log.error("异常信息:", e);
                      }
                    }
                    return Flux.just(s);
                  });
    }
    pipelineFlux =
        pipelineFlux
            .transform((Function<? super Flux, Flux>) component.execute())
            .contextWrite(
                ctx -> {
                  String version = "13";
                  try {
                    version = getIotdbVersion(pipelineContext.getModel().getSession());
                  } catch (StatementExecutionException | IoTDBConnectionException e) {
                    log.error("获取iotdb version异常", e);
                  }
                  ctx = ((Context) ctx).put("pipelineContext", pipelineContext);
                  ctx = ((Context) ctx).put("VERSION", version);
                  return ctx;
                });

    if (it.hasNext()) {
      disposable = pipelineFlux.subscribe();
      disposableList.add(disposable);
      return syncRun(it, disposable);
    } else {
      disposable =
          pipelineFlux
              .doOnError(
                  e -> {
                    log.error("异常信息:", e);
                    IECommonModel ieCommonModel = pipelineContext.getModel();
                    if (ieCommonModel.getE() != null) {
                      ieCommonModel.getE().accept((Throwable) e);
                    }
                  })
              .doFinally(
                  type -> {
                    SignalType signalType = (SignalType) type;
                    // 提供回调
                    IECommonModel ieCommonModel = pipelineContext.getModel();
                    if (ieCommonModel.getConsumer() != null) {
                      ieCommonModel.getConsumer().accept(signalType);
                    }
                  })
              .subscribe();
      disposableList.add(disposable);
      return disposable;
    }
  }

  private String getIotdbVersion(Session session)
      throws StatementExecutionException, IoTDBConnectionException {
    String versionSql = "show version";
    SessionDataSet dataSet = session.executeQueryStatement(versionSql);
    String version = dataSet.next().getFields().get(0).getStringValue();
    if (version.startsWith("0.13")) {
      return "13";
    } else if (version.startsWith("0.12")) {
      return "12";
    } else {
      throw new IllegalArgumentException(
          "not supported iotdb version, support version is v12 or v13");
    }
  }

  /**
   * 暂时没用到 某些情况，需要等到任务执行结果，参考代码 while(true){ List flags = disposableList.stream().map(s->{ return
   * s.isDisposed(); }).collect(Collectors.toList()); if(!flags.contains(false)){ return; } try{
   * Thread.sleep(1000); } catch (InterruptedException e) { } }
   *
   * @return
   */
  private List<Disposable> startParallel() {
    List<Disposable> disposableList =
        componentChain
            .parallelStream()
            .map(
                wrapper -> {
                  Flux pipelineFlux =
                      Flux.just("------------------pipeline start------------- \r\n");
                  pipelineFlux =
                      pipelineFlux.transform((Function<? super Flux, Flux>) wrapper.execute());
                  Disposable disposable =
                      pipelineFlux
                          .contextWrite(
                              ctx -> ((Context) ctx).put("pipelineContext", pipelineContext))
                          .doOnTerminate(() -> log.info("pipeline complate!"))
                          .subscribe();
                  return disposable;
                })
            .collect(Collectors.toList());
    return disposableList;
  }

  @Override
  public void shutDown() {
    disposableList.forEach(
        disposable -> {
          disposable.dispose();
        });
  }

  public CommonPipeline withContext(PipelineContext context) {
    this.pipelineContext = pipelineContext;
    return this;
  }

  public CommonPipeline withContext(Supplier<PipelineContext> supplier) {
    this.pipelineContext = supplier.get();
    return this;
  }

  public List<Component> getComponentChain() {
    return componentChain;
  }

  public void setComponentChain(List<Component> componentChain) {
    this.componentChain = componentChain;
  }

  public PipelineContext<IECommonModel> getPipelineContext() {
    return pipelineContext;
  }

  public void setPipelineContext(PipelineContext<IECommonModel> pipelineContext) {
    this.pipelineContext = pipelineContext;
  }

  public List<Disposable> getDisposableList() {
    return disposableList;
  }

  public void setDisposableList(List<Disposable> disposableList) {
    this.disposableList = disposableList;
  }
}
