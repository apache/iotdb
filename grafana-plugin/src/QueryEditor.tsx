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
  isDropDownList: boolean;
  sqlType: string;
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
const selectType = ['SQL: Full Customized', 'SQL: Drop-down List'];
const commonOption: SelectableValue<string> = { label: '*', value: '*' };
const commonOptionDou: SelectableValue<string> = { label: '**', value: '**' };
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
    isDropDownList: false,
    sqlType: selectType[0],
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

  
  onSelectTypeChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query } = this.props;
    onChange({ ...query });
  };

  onTimeSeriesChange = (t: string[], options: Array<Array<SelectableValue<string>>>, isRemove: boolean) => {
    const { onChange, query } = this.props;
    const commonOption: SelectableValue<string> = { label: '*', value: '*' };
    if (t.length === options.length) {
      this.props.datasource
        .nodeQuery(['root', ...t])
        .then((a) => {
          let b = a.map((a) => a.text).map(toOption);
          if (b.length > 0) {
            b = [commonOption, commonOptionDou, ...b];
          }
          onChange({ ...query, paths: t, options: [...options, b] });
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
    if (this.props.query.sqlType) {
      this.setState({ isDropDownList: this.props.query.isDropDownList, sqlType: this.props.query.sqlType });
    } else {
      this.props.query.sqlType = selectType[0];
    }
    if (this.state.options.length === 1 && this.state.options[0][0].value === '') {
      this.props.datasource.nodeQuery(['root']).then((a) => {
        let b = a.map((a) => a.text).map(toOption);
        if (b.length > 0) {
          b = [commonOption, commonOptionDou, ...b];
        }
        this.setState({ options: [b] });
      });
    }
  }

  render() {
    const query = defaults(this.props.query);
    let { expression, prefixPath, condition, control, fillClauses, aggregateFun, paths, options, sqlType, groupBy } =
      query;
    return (
      <>
        {
          <>
            <div className="gf-form">
              <Segment
                onChange={({ value: value = selectType[0] }) => {
                  const { onChange, query } = this.props;
                  if (value === selectType[0]) {
                    this.props.query.sqlType = selectType[0];
                    this.props.query.aggregateFun = '';
                    if (this.props.query.paths) {
                      const nextTimeSeries = this.props.query.paths.filter((_, i) => i < 0);
                      const nextOptions = this.props.query.options.filter((_, i) => i < 0);
                      this.onTimeSeriesChange(nextTimeSeries, nextOptions, true);
                    }
                    if (this.props.query.groupBy?.samplingInterval) {
                      this.props.query.groupBy.samplingInterval = '';
                    }
                    if (this.props.query.groupBy?.groupByLevel) {
                      this.props.query.groupBy.groupByLevel = '';
                    }
                    if (this.props.query.groupBy?.step) {
                      this.props.query.groupBy.step = '';
                    }
                    this.props.query.condition = '';
                    this.props.query.fillClauses = '';
                    this.props.query.isDropDownList = false;
                    this.setState({
                      isDropDownList: false,
                      sqlType: selectType[0],
                      shouldAdd: true,
                      aggregateFun: '',
                      fillClauses: '',
                      condition: '',
                    });
                    onChange({ ...query, sqlType: value, isDropDownList: false });
                  } else {
                    this.props.query.sqlType = selectType[1];
                    this.props.query.expression = [''];
                    this.props.query.prefixPath = [''];
                    this.props.query.condition = '';
                    this.props.query.control = '';
                    this.props.query.isDropDownList = true;
                    this.setState({
                      isDropDownList: true,
                      sqlType: selectType[1],
                      expression: [''],
                      prefixPath: [''],
                      condition: '',
                      control: '',
                    });
                    onChange({ ...query, sqlType: value, isDropDownList: true });
                  }
                }}
                options={selectType.map(toOption)}
                value={sqlType ? sqlType : this.state.sqlType}
                className="query-keyword width-10"
              />
            </div>
            {!this.state.isDropDownList && (
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
                    <WhereValue
                      condition={condition ? condition : this.state.condition}
                      onChange={this.onWhereValueChange}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'CONTROL'}>
                    <ControlValue
                      control={control ? control : this.state.control}
                      onChange={this.onControlValueChange}
                    />
                  </QueryInlineField>
                </div>
              </>
            )}
            {this.state.isDropDownList && (
              <>
                <div className="gf-form">
                  <QueryInlineField label={'TIME-SERIES'}>
                    <TimeSeries
                      timeSeries={paths ? paths : this.state.timeSeries}
                      onChange={this.onTimeSeriesChange}
                      variableOptionGroup={options ? options : this.state.options}
                      shouldAdd={this.state.shouldAdd}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'FUNCTION'}>
                    <AggregateFun
                      aggregateFun={aggregateFun ? aggregateFun : this.state.aggregateFun}
                      onChange={this.onAggregationsChange}
                      variableOptionGroup={selectElement.map(toOption)}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'WHERE'}>
                    <WhereValue
                      condition={condition ? condition : this.state.condition}
                      onChange={this.onWhereValueChange}
                    />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'GROUP BY'}>
                    <QueryField label={'SAMPLING INTERVAL'} />
                    <GroupByLabel groupBy={groupBy ? groupBy : this.state.groupBy} onChange={this.onGroupByChange} />
                  </QueryInlineField>
                </div>
                <div className="gf-form">
                  <QueryInlineField label={'FILL'}>
                    <FillValue
                      fill={fillClauses ? fillClauses : this.state.fillClauses}
                      onChange={this.onFillsChange}
                    />
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
