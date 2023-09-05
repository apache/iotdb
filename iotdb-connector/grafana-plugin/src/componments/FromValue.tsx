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
import { HorizontalGroup, Icon, SegmentInput, VerticalGroup } from '@grafana/ui';
import { QueryInlineField } from './Form';

export interface Props {
  onChange: (prefixPath: string[]) => void;
  prefixPath: string[];
}

export const FromValue: FunctionComponent<Props> = ({ prefixPath, onChange }) => (
  <>
    {prefixPath &&
      prefixPath.map((value, index) => (
        <>
          {index === 0 && (
            <SegmentInput
              onChange={(value) => {
                onChange(prefixPath.map((v, i) => (i === index ? value.toString() : v)));
              }}
              value={value}
              className="min-width-8"
            />
          )}
          {index === 0 && prefixPath.length === 1 && (
            <a
              className="gf-form-label query-part"
              onClick={() => {
                prefixPath[prefixPath.length] = '';
                onChange(prefixPath);
              }}
            >
              <Icon name="plus" />
            </a>
          )}
          {index === 0 && prefixPath.length > 1 && (
            <a
              itemID={index.toString()}
              className="gf-form-label query-part"
              onClick={(_) => {
                prefixPath.splice(index, 1);
                onChange(prefixPath);
              }}
            >
              <Icon name="minus" />
            </a>
          )}
        </>
      ))}
    <VerticalGroup spacing="xs">
      {prefixPath &&
        prefixPath.map((value, index) => (
          <>
            {index > 0 && (
              <HorizontalGroup spacing="xs">
                <QueryInlineField label={''}>
                  <SegmentInput
                    onChange={(value) => {
                      onChange(prefixPath.map((v, i) => (i === index ? value.toString() : v)));
                    }}
                    value={value}
                    className="min-width-8"
                  />
                  {prefixPath.length > 1 && (
                    <a
                      itemID={index.toString()}
                      className="gf-form-label query-part"
                      onClick={(_) => {
                        prefixPath.splice(index, 1);
                        onChange(prefixPath);
                      }}
                    >
                      <Icon name="minus" />
                    </a>
                  )}
                  {index > 0 && index + 1 === prefixPath.length && (
                    <a
                      className="gf-form-label query-part"
                      onClick={() => {
                        prefixPath[prefixPath.length] = '';
                        onChange(prefixPath);
                      }}
                    >
                      <Icon name="plus" />
                    </a>
                  )}
                </QueryInlineField>
              </HorizontalGroup>
            )}
          </>
        ))}
    </VerticalGroup>
  </>
);
