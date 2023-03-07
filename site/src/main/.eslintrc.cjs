/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/* eslint-env node */
require('@rushstack/eslint-patch/modern-module-resolution');

module.exports = {
  root: true,
  extends: [
    'plugin:vue/vue3-essential',
    '@vue/eslint-config-airbnb-with-typescript',
    // '@vue/eslint-config-airbnb-with-typescript/allow-js-in-vue',
  ],
  parserOptions: {
    ecmaVersion: 'latest',
  },
  rules: {
    'import/prefer-default-export': 'off',
    'import/no-extraneous-dependencies': 'off',
    'vue/multi-word-component-names': 'off',
    '@typescript-eslint/no-empty-function': 'off',
    '@typescript-eslint/no-var-requires': 'off',
    'no-param-reassign': 'off', // param reassign
    'no-plusplus': 'off', // ++
    'vue/require-default-prop': 'off', // default prop todo
    'vuejs-accessibility/click-events-have-key-events': 'off',
    'no-console': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
    'vue/html-self-closing': ['error', {
      html: {
        void: 'never',
        normal: 'never',
        component: 'always',
      },
      svg: 'always',
      math: 'always',
    }],
    'vue/max-len': [
      'error',
      200,
      2,
      {
        ignoreUrls: true,
        ignoreComments: false,
        ignoreRegExpLiterals: true,
        ignoreStrings: true,
        ignoreTemplateLiterals: true,

        // 1. it's like `ignoreStrings`
        // 2. SVG `path`s should be ignored
        ignoreHTMLAttributeValues: true,
        // Because spaces in HTML are insignificant,
        // it shouldn't be hard to start a new line for text content
        ignoreHTMLTextContents: false,
      },
    ],
  },
};
