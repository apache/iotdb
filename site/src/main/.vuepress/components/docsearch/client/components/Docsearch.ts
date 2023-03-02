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

/* eslint-disable no-underscore-dangle */
/* eslint-disable @typescript-eslint/naming-convention */
// eslint-disable-next-line import/no-named-default
import { default as docsearch } from '@docsearch/js';
import { usePageLang, useRouteLocale, usePageData } from '@vuepress/client';
import { isArray } from '@vuepress/shared';
import {
  computed, defineComponent, h, onMounted, watch,
} from 'vue';
import type { PropType } from 'vue';
import type { DocsearchOptions } from '../../shared/index.js';
import { useDocsearchShim } from '../composables/index.js';

declare const __DOCSEARCH_INJECT_STYLES__: boolean;
declare const __DOCSEARCH_OPTIONS__: DocsearchOptions;
const options = __DOCSEARCH_OPTIONS__;

if (__DOCSEARCH_INJECT_STYLES__) {
  import('@docsearch/css');
  import('../styles/docsearch.css');
}

export const Docsearch = defineComponent({
  name: 'Docsearch',

  props: {
    containerId: {
      type: String,
      required: false,
      default: 'docsearch-container',
    },
    options: {
      type: Object as PropType<DocsearchOptions>,
      required: false,
      default: () => options,
    },
  },

  setup(props) {
    const routeLocale = useRouteLocale();
    const lang = usePageLang();
    const docsearchShim = useDocsearchShim();
    const pageData = usePageData();

    // resolve docsearch options for current locale
    const optionsLocale = computed(() => ({
      ...props.options,
      ...props.options.locales?.[routeLocale.value],
    }));

    const getDocVersion = (branch = 'master', path = '') => {
      if (path.indexOf('UserGuide/Master') > -1 || path.indexOf('UserGuide') === -1) {
        return branch;
      }
      const branchRex = /UserGuide\/V(\d+\.\d+\.x)/;
      if (branchRex.test(path)) {
        const tag = branchRex.exec(path)![1];
        return `rel/${tag.replace('.x', '')}`;
      }
      console.log('branch', branch);
      return branch;
    };

    const facetFilters: string[] = [];

    const initialize = (): void => {
      const rawFacetFilters = optionsLocale.value.searchParameters?.facetFilters ?? [];
      facetFilters.splice(
        0,
        facetFilters.length,
        `lang:${lang.value}`,
        `version:${getDocVersion('master', pageData.value.path)}`,
        ...(isArray(rawFacetFilters) ? rawFacetFilters : [rawFacetFilters]),
      );
      // @ts-expect-error: https://github.com/microsoft/TypeScript/issues/50690
      docsearch({
        ...docsearchShim,
        ...optionsLocale.value,
        container: `#${props.containerId}`,
        searchParameters: {
          ...optionsLocale.value.searchParameters,
          facetFilters,
        },
      });
    };

    onMounted(() => {
      initialize();

      // re-initialize if the options is changed
      watch(
        [routeLocale, optionsLocale],
        (
          [curRouteLocale, curPropsLocale],
          [prevRouteLocale, prevPropsLocale],
        ) => {
          if (curRouteLocale === prevRouteLocale) return;
          if (
            JSON.stringify(curPropsLocale) !== JSON.stringify(prevPropsLocale)
          ) {
            initialize();
          }
        },
      );

      // modify the facetFilters in place to avoid re-initializing docsearch
      // when page lang is changed
      watch(lang, (curLang, prevLang) => {
        if (curLang !== prevLang) {
          const prevIndex = facetFilters.findIndex(
            (item) => item === `lang:${prevLang}`,
          );
          if (prevIndex > -1) {
            facetFilters.splice(prevIndex, 1, `lang:${curLang}`);
          }
        }
      });

      watch(pageData, (cur, prev) => {
        const newVersion = getDocVersion('master', cur.path);
        const oldVersion = getDocVersion('master', prev.path);
        if (newVersion !== oldVersion) {
          const prevIndex = facetFilters.findIndex(
            (item) => item === `version:${oldVersion}`,
          );
          if (prevIndex > -1) {
            facetFilters.splice(prevIndex, 1, `version:${newVersion}`);
          } else {
            facetFilters.push(`version:${newVersion}`);
          }
        }
      });
    });

    return () => h('div', { id: props.containerId });
  },
});
