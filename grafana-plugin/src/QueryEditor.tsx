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
import defaults from 'lodash/defaults';
import React, { ChangeEvent, PureComponent } from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from './datasource';
import { GroupBy, IoTDBOptions, IoTDBQuery } from './types';
import { QueryField, QueryInlineField } from './componments/Form';
import { TimeSeries } from './componments/TimeSeries';
import { SelectValue } from './componments/SelectValue';
import { FromValue } from './componments/FromValue';
import { WhereValue } from './componments/WhereValue';
import { ControlValue } from './componments/ControlValue';
import { FillValue } from './componments/FillValue';
import { Segment } from '@grafana/ui';
import { toOption } from './functions';

import { GroupByLabel } from './componments/GroupBy';
import { AggregateFun } from './componments/AggregateFun';

interface State {
  expression: string[];
  prefixPath: string[];
  condition: string;
  control: string;

  timeSeries: string[];
  options: Array<Array<SelectableValue<string>>>;
  aggregateFun: string;
  groupBy: GroupBy;
  fillClauses: string;
  isAggregated: boolean;
  aggregated: string;
  shouldAdd: boolean;
}

const selectElement = [
  '---remove---',
  'SUM',
  'COUNT',
  'AVG',
  'EXTREME',
  'MAX_VALUE',
  'MIN_VALUE',
  'FIRST_VALUE',
  'LAST_VALUE',
  'MAX_TIME',
  'MIN_TIME',
];

const paths = [''];
const expressions = [''];
const selectRaw = ['Raw', 'Aggregation'];
type Props = QueryEditorProps<DataSource, IoTDBQuery, IoTDBOptions>;

export class QueryEditor extends PureComponent<Props, State> {
  state: State = {
    expression: expressions,
    prefixPath: paths,
    condition: '',
    control: '',

    timeSeries: [],
    options: [[toOption('')]],
    aggregateFun: '',
    groupBy: {
      samplingInterval: '',
      step: '',
      groupByLevel: '',
    },
    fillClauses: '',
    isAggregated: false,
    aggregated: selectRaw[0],
    shouldAdd: true,
  };

  onSelectValueChange = (exp: string[]) => {
    const { onChange, query } = this.props;
    this.setState({ expression: exp });
    onChange({ ...query, expression: exp });
  };

  onFromValueChange = (p: string[]) => {
    const { onChange, query } = this.props;
    this.setState({ prefixPath: p });
    onChange({ ...query, prefixPath: p });
  };

  onWhereValueChange = (c: string) => {
    const { onChange, query } = this.props;
    onChange({ ...query, condition: c });
    this.setState({ condition: c });
  };
  onControlValueChange = (c: string) => {
    const { onChange, query } = this.props;
    onChange({ ...query, control: c });
    this.setState({ control: c });
  };

  onAggregationsChange = (a: string) => {
    const { onChange, query } = this.props;
    if (a === '---remove---') {
      a = '';
    }
    this.setState({ aggregateFun: a });
    onChange({ ...query, aggregateFun: a });
  };

  onFillsChange = (f: string) => {
    const { onChange, query } = this.props;
    onChange({ ...query, fillClauses: f });
    this.setState({ fillClauses: f });
  };

  onGroupByChange = (g: GroupBy) => {
    const { onChange, query } = this.props;
    this.setState({ groupBy: g });
    onChange({ ...query, groupBy: g });
  };

  onQueryTextChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query } = this.props;
    onChange({ ...query });
  };

  onTimeSeriesChange = (t: string[], options: Array<Array<SelectableValue<string>>>, isRemove: boolean) => {
    const { onChange, query } = this.props;
    if (t.length === options.length) {
      this.props.datasource
        .nodeQuery(['root', ...t])
        .then((a) => {
          const b = a.map((a) => a.text).map(toOption);
          onChange({ ...query, paths: t });
          if (isRemove) {
            this.setState({ timeSeries: t, options: [...options, b], shouldAdd: true });
          } else {
            this.setState({ timeSeries: t, options: [...options, b] });
          }
        })
        .catch((e) => {
          if (e === 'measurement') {
            onChange({ ...query, paths: t });
            this.setState({ timeSeries: t, shouldAdd: false });
          } else {
            this.setState({ shouldAdd: false });
          }
        });
    } else {
      this.setState({ timeSeries: t });
      onChange({ ...query, paths: t });
    }
  };

  componentDidMount() {
    this.props.query.aggregated = selectRaw[0];
    if (this.state.options.length === 1 && this.state.options[0][0].value === '') {
      this.props.datasource.nodeQuery(['root']).then((a) => {
        const b = a.map((a) => a.text).map(toOption);
        this.setState({ options: [b] });
      });
    }
  }

  render() {
    const query = defaults(this.props.query);
    const { expression, prefixPath, condition, control, fillClauses } = query;

    return (
      <>
        {
          <>
            <div className="gf-form">
              <Segment
                onChange={({ value: value = '' }) => {
                  if (value === selectRaw[0]) {
                    this.props.query.aggregated = selectRaw[0];
                    this.setState({ isAggregated: false, aggregated: selectRaw[0] });
                  } else {
                    this.props.query.aggregated = selectRaw[1];
                    this.setState({ isAggregated: true, aggregated: selectRaw[1] });
                  }
                }}
                options={selectRaw.map(toOption)}
                value={this.state.aggregated}
                className="query-keyword width-6"
              />
            </div>
            {!this.state.isAggregated && (
              <>
                <div className="gf-form">
                  <QueryInlineField label={'SELECT'}>
                    <SelectValue
                      expressions={expression ? expression : this.state.expression}
                      onChange={this.onSelectValueChange}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'FROM'}>
                    <FromValue
                      prefixPath={prefixPath ? prefixPath : this.state.prefixPath}
                      onChange={this.onFromValueChange}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'WHERE'}>
                    <WhereValue condition={condition} onChange={this.onWhereValueChange} />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'CONTROL'}>
                    <ControlValue control={control} onChange={this.onControlValueChange} />
                  </QueryInlineField>
                </div>
              </>
            )}
            {this.state.isAggregated && (
              <>
                <div className="gf-form">
                  <QueryInlineField label={'TIME-SERIES'}>
                    <TimeSeries
                      timeSeries={this.state.timeSeries}
                      onChange={this.onTimeSeriesChange}
                      variableOptionGroup={this.state.options}
                      shouldAdd={this.state.shouldAdd}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'FUNCTION'}>
                    <AggregateFun
                      aggregateFun={this.state.aggregateFun}
                      onChange={this.onAggregationsChange}
                      variableOptionGroup={selectElement.map(toOption)}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'WHERE'}>
                    <WhereValue condition={condition} onChange={this.onWhereValueChange} />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'GROUP BY'}>
                    <QueryField label={'SAMPLING INTERVAL'} />
                    <GroupByLabel groupBy={this.state.groupBy} onChange={this.onGroupByChange} />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'FILL'}>
                    <FillValue fill={fillClauses} onChange={this.onFillsChange} />
                  </QueryInlineField>
                </div>
              </>
            )}
          </>
        }
      </>
    );
  }
}
