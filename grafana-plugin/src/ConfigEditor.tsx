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
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { IoTDBOptions, IoTDBSecureJsonData } from './types';

interface Props extends DataSourcePluginOptionsEditorProps<IoTDBOptions, IoTDBSecureJsonData> {}

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

  onUserNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      username: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  // Secure field (only sent to the backend)
  onPasswordChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonData: {
        password: event.target.value,
      },
    });
  };

  onResetPassword = () => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        password: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        password: '',
      },
    });
  };

  render() {
    const { options } = this.props;
    const { secureJsonFields, jsonData } = options;
    const secureJsonData = (options.secureJsonData || {}) as IoTDBSecureJsonData;

    return (
      <div className="gf-form-group">
        <InlineField label="URL" labelWidth={12}>
          <Input
            onChange={this.onURLChange}
            value={jsonData.url || ''}
            placeholder="please input URL"
            width={40}
          />
        </InlineField>
        <InlineField label="username" labelWidth={12}>
          <Input
            onChange={this.onUserNameChange}
            value={jsonData.username || ''}
            placeholder="please input username"
            width={40}
          />
        </InlineField>
        <InlineField label="password" labelWidth={12}>
          <SecretInput
            isConfigured={(secureJsonFields && secureJsonFields.password) as boolean}
            value={secureJsonData.password || ''}
            placeholder="please input password"
            width={40}
            onReset={this.onResetPassword}
            onChange={this.onPasswordChange}
          />
        </InlineField>
      </div>

    );
  }
}
