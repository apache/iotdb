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

import React, { FunctionComponent } from 'react';
import { SelectableValue } from '@grafana/data';
import { Segment, Icon, InlineFormLabel } from '@grafana/ui';

export interface Props {
  timeSeries: string[];
  onChange: (path: string[], options: Array<Array<SelectableValue<string>>>, isRemove: boolean) => void;
  variableOptionGroup: Array<Array<SelectableValue<string>>>;
  shouldAdd: boolean;
}

const removeText = '-- remove stat --';
const removeOption: SelectableValue<string> = { label: removeText, value: removeText };

export const TimeSeries: FunctionComponent<Props> = ({ timeSeries, onChange, variableOptionGroup, shouldAdd }) => {
  return (
    <>
      <>
        <InlineFormLabel width={3}>root</InlineFormLabel>
      </>
      {timeSeries &&
        timeSeries.map((value, index) => (
          <>
            <Segment
              allowCustomValue={false}
              key={value + index}
              value={value}
              options={[removeOption, ...variableOptionGroup[index]]}
              onChange={({ value: selectValue = '' }) => {
                if (selectValue === removeText) {
                  const nextTimeSeries = timeSeries.filter((_, i) => i < index);
                  const nextOptions = variableOptionGroup.filter((_, i) => i < index);
                  onChange(nextTimeSeries, nextOptions, true);
                } else if (selectValue !== value) {
                  const nextTimeSeries = timeSeries
                    .map((v, i) => (i === index ? selectValue : v))
                    .filter((_, i) => i <= index);
                  const nextOptions = variableOptionGroup.filter((_, i) => i <= index);
                  onChange(nextTimeSeries, nextOptions, true);
                }
              }}
            />
          </>
        ))}
      {shouldAdd && (
        <Segment
          Component={
            <a className="gf-form-label query-part">
              <Icon name="plus" />
            </a>
          }
          allowCustomValue
          onChange={(item: SelectableValue<string>) => {
            let itemString = '';
            if (item.value) {
              itemString = item.value;
            }
            onChange([...timeSeries, itemString], variableOptionGroup, false);
          }}
          options={variableOptionGroup[variableOptionGroup.length - 1]}
        />
      )}
    </>
  );
};
