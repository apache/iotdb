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
import { DataSourceInstanceSettings, MetricFindValue, ScopedVars } from '@grafana/data';

import { IoTDBOptions, IoTDBQuery } from './types';
import { toMetricFindValue } from './functions';
import { DataSourceWithBackend, getBackendSrv, getTemplateSrv } from '@grafana/runtime';

export class DataSource extends DataSourceWithBackend<IoTDBQuery, IoTDBOptions> {
  username: string;
  password: string;
  url: string;

  constructor(instanceSettings: DataSourceInstanceSettings<IoTDBOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.jsonData.url;
    this.password = instanceSettings.jsonData.password;
    this.username = instanceSettings.jsonData.username;
  }
  applyTemplateVariables(query: IoTDBQuery, scopedVars: ScopedVars) {
    query.expression.map(
      (_, index) => (query.expression[index] = getTemplateSrv().replace(query.expression[index], scopedVars))
    );
    query.prefixPath.map(
      (_, index) => (query.prefixPath[index] = getTemplateSrv().replace(query.prefixPath[index], scopedVars))
    );
    if (query.condition) {
      query.condition = getTemplateSrv().replace(query.condition, scopedVars);
    }
    if (query.control) {
      query.control = getTemplateSrv().replace(query.control, scopedVars);
    }
    return query;
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
        url: this.url + '/grafana/v1/variable',
        data: sql,
        headers: myHeader,
      })
      .then((response) => {
        if (response.data instanceof Array) {
          return response.data;
        } else {
          throw 'the result is not array';
        }
      })
      .then((data) => data.map(toMetricFindValue));
  }
}
