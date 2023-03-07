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

import type { DocSearchProps } from '@docsearch/react';
import { resolveRoutePathFromUrl } from '@vuepress/shared';
import { debounce } from 'ts-debounce';
import { useRouter } from 'vue-router';

// eslint-disable-next-line @typescript-eslint/naming-convention, no-underscore-dangle
declare const __DOCSEARCH_INDEX_BASE__: string;

const isSpecialClick = (event: MouseEvent): boolean => event.button === 1
  || event.altKey
  || event.ctrlKey
  || event.metaKey
  || event.shiftKey;

/**
 * Get docsearch options to be compatible with VuePress
 */
export const useDocsearchShim = (): Partial<DocSearchProps> => {
  const router = useRouter();

  return {
    // render the hit component with custom `onClick` handler
    hitComponent: ({ hit, children }) => ({
      type: 'a',
      ref: undefined,
      constructor: undefined,
      key: undefined,
      props: {
        href: hit.url,
        // handle `onClick` by `router.push`
        onClick: (event: MouseEvent) => {
          if (isSpecialClick(event)) {
            return;
          }
          event.preventDefault();
          router.push(
            resolveRoutePathFromUrl(hit.url, __DOCSEARCH_INDEX_BASE__),
          );
        },
        children,
      },
      __v: null,
    } as unknown),

    // navigation behavior triggered by `onKeyDown` internally
    navigator: {
      // when pressing Enter without metaKey
      navigate: ({ itemUrl }) => {
        router.push(resolveRoutePathFromUrl(itemUrl, __DOCSEARCH_INDEX_BASE__));
      },
    },

    // add search debounce
    transformSearchClient: (searchClient) => {
      const searchWithDebounce = debounce(searchClient.search, 500);
      return {
        ...searchClient,
        search: async (...args) => searchWithDebounce(...args),
      };
    },
  } as Partial<DocSearchProps>;
};
