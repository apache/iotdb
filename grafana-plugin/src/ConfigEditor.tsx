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
import React, { ChangeEvent, PureComponent } from 'react';
import { LegacyForms } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { IoTDBOptions } from './types';

const { FormField } = LegacyForms;

interface Props extends DataSourcePluginOptionsEditorProps<IoTDBOptions> {}

interface State {}

export class ConfigEditor extends PureComponent<Props, State> {
  onURLChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      url: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onUserChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      username: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onPassWordChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      password: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  render() {
    const { options } = this.props;
    const { jsonData } = options;

    return (
      <div className="gf-form-group">
        <div className="gf-form">
          <FormField
            label="URL"
            labelWidth={6}
            inputWidth={20}
            onChange={this.onURLChange}
            value={jsonData.url || ''}
            placeholder="please input URL"
          />
        </div>

        <div className="gf-form-inline">
          <div className="gf-form">
            <FormField
              value={jsonData.username || ''}
              label="username"
              placeholder="please input username"
              labelWidth={6}
              inputWidth={20}
              onChange={this.onUserChange}
            />
          </div>
        </div>

        <div className="gf-form-inline">
          <div className="gf-form">
            <FormField
              value={jsonData.password || ''}
              label="password"
              type="password"
              placeholder="please input password"
              labelWidth={6}
              inputWidth={20}
              onChange={this.onPassWordChange}
            />
          </div>
        </div>
      </div>
    );
  }
}
