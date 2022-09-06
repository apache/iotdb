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

import org.apache.iotdb.backup.core.pipeline.context.model.DeleteModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** @Author: LL @Description: @Date: create in 2022/7/26 13:51 */
@Slf4j
public class DeleteStarter implements Starter<DeleteModel> {

  @Override
  public Disposable start(DeleteModel model) {
    Scheduler scheduler = Schedulers.single();
    Disposable disposable =
        Flux.just("")
            .subscribeOn(scheduler)
            .flatMap(
                s -> {
                  deleteTimeseries(model);
                  return Flux.just(s);
                })
            .doFinally(
                s -> {
                  scheduler.dispose();
                })
            .subscribe();
    return disposable;
  }

  public void deleteTimeseries(DeleteModel model) {
    String version = getIotdbVersion(model.getSession());
    if (model.getMeasurementList() != null && model.getMeasurementList().size() != 0) {
      model
          .getMeasurementList()
          .forEach(
              s -> {
                StringBuilder sql = new StringBuilder();
                sql.append("delete from ")
                    .append(ExportPipelineService.formatPath(model.getIotdbPath(), version))
                    .append(".")
                    .append(ExportPipelineService.formatMeasurement(s));
                if (model.getWhereClause() != null && !"".equals(model.getWhereClause())) {
                  sql.append(" where ").append(model.getWhereClause());
                }
                doExecuteQuery(model.getSession(), sql.toString());
              });
    } else {
      StringBuilder sql = new StringBuilder();
      sql.append("delete  from ").append(ExportPipelineService.formatPath(model.getIotdbPath()));
      if (model.getWhereClause() != null && !"".equals(model.getWhereClause())) {
        sql.append(" where ").append(model.getWhereClause());
      }
      doExecuteQuery(model.getSession(), sql.toString());
    }
  }

  public void doExecuteQuery(Session session, String sql) {
    try {
      session.executeNonQueryStatement(sql);
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      log.error("异常SQL:{}\n异常信息:", sql, e);
    }
  }

  private String getIotdbVersion(Session session) {
    try {
      String versionSql = "show version";
      SessionDataSet dataSet = session.executeQueryStatement(versionSql);
      String version = dataSet.next().getFields().get(0).getStringValue();
      if (version.startsWith("0.13")) {
        return "13";
      } else if (version.startsWith("0.12")) {
        return "12";
      } else {
        return version;
      }
    } catch (Exception e) {
      log.error("获取版本异常", e);
      return null;
    }
  }

  @Override
  public void shutDown() {}

  @Override
  public Double[] rateOfProcess() {
    return new Double[0];
  }

  @Override
  public Long finishedRowNum() {
    return null;
  }

  // public static void main(String[] args) throws IoTDBConnectionException, InterruptedException {
  // DeleteModel deleteModel = new DeleteModel();
  // deleteModel.setIotdbPath("root.test.yonyou.cli.**");
  // deleteModel.setWhereClause("time < 1657072800000");
  // List<String> dd = new ArrayList<>();
  // dd.add("targetHost");
  // dd.add("providerId");
  // deleteModel.setMeasurementList(dd);
  //
  // Session session = new Session("127.0.0.1",6667,"root","root");
  // session.open();
  // deleteModel.setSession(session);
  //
  // Starter starter = new DeleteStarter();
  // Disposable disposable = starter.start(deleteModel);
  // while (!disposable.isDisposed()){
  // Thread.sleep(1000);
  // }
  // session.close();
  // }
}
