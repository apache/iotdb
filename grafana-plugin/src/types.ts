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
import { DataQuery, DataSourceJsonData, SelectableValue } from '@grafana/data';

export interface IoTDBQuery extends DataQuery {
  startTime: number;
  endTime: number;
  expression: string[];
  prefixPath: string[];
  condition: string;
  control: string;

  paths: string[];
  aggregateFun?: string;
  sqlType: string;
  isDropDownList: boolean;
  fillClauses: string;
  groupBy?: GroupBy;
  limitAll?: LimitAll;
  options: Array<Array<SelectableValue<string>>>;
  hide: boolean;
}

export interface GroupBy {
  step: string;
  samplingInterval: string;
  groupByLevel: string;
}

export interface Fill {
  dataType: string;
  previous: string;
  duration: string;
}

export interface LimitAll {
  slimit: string;
  limit: string;
}

/**
 * These are options configured for each DataSource instance
 */
export interface IoTDBOptions extends DataSourceJsonData {
  url: string;
  username: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface IoTDBSecureJsonData {
  password?: string;
}
