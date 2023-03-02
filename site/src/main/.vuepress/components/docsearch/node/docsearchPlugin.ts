/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
 */

import type { Plugin } from '@vuepress/core';
import { getDirname, path } from '@vuepress/utils';
import type { DocsearchOptions } from '../shared/index.js';

// eslint-disable-next-line @typescript-eslint/naming-convention, no-underscore-dangle
const __dirname = getDirname(import.meta.url);

/**
 * Options for @vuepress/plugin-docsearch-iotdb
 */
export interface DocsearchPluginOptions extends DocsearchOptions {
  /**
   * Base path of the search index
   */
  indexBase?: string

  /**
   * Whether to inject docsearch default styles
   */
  injectStyles?: boolean
}

export const docsearchPlugin = ({
  injectStyles = true,
  indexBase,
  ...options
}: DocsearchPluginOptions): Plugin => ({
  name: '@vuepress/plugin-docsearch-iotdb',

  clientConfigFile: path.resolve(__dirname, '../client/config.js'),

  define: (app) => ({
    __DOCSEARCH_INJECT_STYLES__: injectStyles,
    __DOCSEARCH_INDEX_BASE__: indexBase || app.options.base,
    __DOCSEARCH_OPTIONS__: options,
  }),
});
