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
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { IoTDBOptions, IoTDBQuery } from './types';
import { QueryInlineField } from './componments/Form';
import { SelectValue } from 'componments/SelectValue';
import { FromValue } from 'componments/FromValue';
import { WhereValue } from 'componments/WhereValue';
import { ControlValue } from 'componments/ControlValue';

interface State {
  expression: string[];
  prefixPath: string[];
  condition: string;
  control: string;
}

const paths = [''];
const expressions = [''];
type Props = QueryEditorProps<DataSource, IoTDBQuery, IoTDBOptions>;

export class QueryEditor extends PureComponent<Props, State> {
  state: State = {
    expression: expressions,
    prefixPath: paths,
    condition: '',
    control: '',
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

  onQueryTextChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query } = this.props;
    onChange({ ...query, queryText: event.target.value });
  };

  render() {
    const query = defaults(this.props.query);
    const { expression, prefixPath, condition, control } = query;

    return (
      <>
        {
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
        }
      </>
    );
  }
}
