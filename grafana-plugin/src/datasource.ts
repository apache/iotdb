/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MetricFindValue,
  toDataFrame,
} from '@grafana/data';

import { MyQuery, MyDataSourceOptions } from './types';
import { toMetricFindValue } from './functions';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';

export class DataSource extends DataSourceApi<MyQuery, MyDataSourceOptions> {
  username: string;
  password: string;
  url: string;

  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.jsonData.url;
    this.password = instanceSettings.jsonData.password;
    this.username = instanceSettings.jsonData.username;
  }

  async query(options: DataQueryRequest<MyQuery>): Promise<DataQueryResponse> {
    const { range } = options;
    const dataFrames = options.targets.map(target => {
      target.startTime = range!.from.valueOf();
      target.endTime = range!.to.valueOf();
      if (options) {
        target.prefixPath.map(
          (_, index) =>
            (target.prefixPath[index] = getTemplateSrv().replace(target.prefixPath[index], options.scopedVars))
        );
        target.expression.map(
          (_, index) =>
            (target.expression[index] = getTemplateSrv().replace(target.expression[index], options.scopedVars))
        );
        if (target.condition) {
          target.condition = getTemplateSrv().replace(target.condition, options.scopedVars);
        }
      }
      //target.paths = ['root', ...target.paths];
      return this.doRequest(target);
    });
    return Promise.all(dataFrames)
      .then(a => a.reduce((accumulator, value) => accumulator.concat(value), []))
      .then(data => ({ data }));
  }

  async doRequest(query: MyQuery) {
    const myHeader = new Headers();
    let reqURL = '/rest/grafana/query/json';
    if (query.expression.length > 0) {
      reqURL = '/grafana/v1/query/expression';
    }
    myHeader.append('Content-Type', 'application/json');
    const Authorization = 'Basic ' + Buffer.from(this.username + ':' + this.password).toString('base64');
    myHeader.append('Authorization', Authorization);
    return await getBackendSrv()
      .datasourceRequest({
        method: 'POST',
        url: this.url + reqURL,
        data: JSON.stringify(query),
        headers: myHeader,
      })
      .then(response => response.data)
      .then(a => {
        if (a instanceof Object && a.expressions !== null) {
          let dataframes: any = [];
          a.expressions.map((v: any, index: any) => {
            let datapoints: any = [];
            if (a.timestamps !== null) {
              a.timestamps.map((time: any, i: any) => {
                datapoints[i] = [a.values[index][i], time];
              });
              const data = { target: v, datapoints: datapoints };
              dataframes[index] = data;
            }
            const data = { target: v, datapoints: datapoints };
            dataframes[index] = data;
          });
          return dataframes.map(toDataFrame);
        } else {
          throw 'the result is not object';
        }
      });
  }

  metricFindQuery(query: any, options?: any): Promise<MetricFindValue[]> {
    query = getTemplateSrv().replace(query, options.scopedVars);
    const sql = { sql: query };
    return this.getVariablesResult(sql);
  }

  async getVariablesResult(sql: object) {
    const myHeader = new Headers();
    myHeader.append('Content-Type', 'application/json');
    const Authorization = 'Basic ' + Buffer.from(this.username + ':' + this.password).toString('base64');
    myHeader.append('Authorization', Authorization);
    return await getBackendSrv()
      .datasourceRequest({
        method: 'POST',
        url: this.url + '/grafana/v1/query',
        data: sql,
        headers: myHeader,
      })
      .then(response => {
        if (response.data instanceof Array) {
          return response.data;
        } else {
          throw 'the result is not array';
        }
      })
      .then(data => data.map(toMetricFindValue));
  }

  async testDatasource() {
    const myHeader = new Headers();
    myHeader.append('Content-Type', 'application/json');
    const Authorization = 'Basic ' + Buffer.from(this.username + ':' + this.password).toString('base64');
    myHeader.append('Authorization', Authorization);
    const response = getBackendSrv().datasourceRequest({
      url: this.url + '/ping',
      method: 'GET',
      headers: myHeader,
    });
    let status = '';
    let message = '';
    await response.then(res => {
      let b = res.data.code === 200 ? true : false;
      if (b) {
        status = 'success';
        message = 'Success';
      } else {
        status = 'error';
        message = res.data.message;
      }
    });
    return {
      status: status,
      message: message,
    };
  }
}
