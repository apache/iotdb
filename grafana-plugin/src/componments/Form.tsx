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

import React, { InputHTMLAttributes, FunctionComponent } from 'react';
import { InlineFormLabel } from '@grafana/ui';

export interface Props extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  tooltip?: string;
  children?: React.ReactNode;
}

export const QueryField: FunctionComponent<Partial<Props>> = ({ label, tooltip, children }) => (
  <>
    <InlineFormLabel width={10} className="query-keyword" tooltip={tooltip}>
      {label}
    </InlineFormLabel>
    {children}
  </>
);

export const QueryInlineField: FunctionComponent<Props> = ({ ...props }) => {
  return (
    <div className={'gf-form-inline'}>
      <QueryField {...props} />
    </div>
  );
};
