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
