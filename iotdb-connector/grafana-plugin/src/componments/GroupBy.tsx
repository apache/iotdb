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

import { GroupBy } from '../types';
import React, { FunctionComponent } from 'react';
import { InlineFormLabel, SegmentInput } from '@grafana/ui';

export interface Props {
  groupBy: GroupBy;
  onChange: (groupBy: GroupBy) => void;
}

export const GroupByLabel: FunctionComponent<Props> = ({ groupBy, onChange }) => (
  <>
    {
      <>
        <SegmentInput
          value={groupBy.samplingInterval}
          onChange={(string) => onChange({ ...groupBy, samplingInterval: string.toString() })}
          className="width-5"
          placeholder="1s"
        />
        <InlineFormLabel className="query-keyword" width={9}>
          SLIDING STEP
        </InlineFormLabel>
        <SegmentInput
          className="width-5"
          placeholder="(optional)"
          value={groupBy.step}
          onChange={(string) => onChange({ ...groupBy, step: string.toString() })}
        />
        <InlineFormLabel className="query-keyword" width={5}>
          LEVEL
        </InlineFormLabel>
        <SegmentInput
          className="width-5"
          placeholder="(optional)"
          value={groupBy.groupByLevel}
          onChange={(string) => onChange({ ...groupBy, groupByLevel: string.toString() })}
        />
      </>
    }
  </>
);
