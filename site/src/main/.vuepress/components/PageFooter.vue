/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

<template>
  <footer style="padding-bottom: 2rem;">
    <span id="doc-version" style="display: none;">{{ docVersion }}</span>
    <p style="text-align: center; color: #909399; font-size: 12px; margin: 0 30px;">Copyright © {{year}} The Apache Software Foundation.<br>
      Apache and the Apache feather logo are trademarks of The Apache Software Foundation</p>
    <p style="text-align: center; margin-top: 10px; color: #909399; font-size: 12px; margin: 0 30px;">
      <strong>Have a question?</strong> Connect with us on QQ, WeChat, or Slack. <a href="https://github.com/apache/iotdb/issues/1995">Join the community</a> now.</p>
    <p style="text-align: center; margin-top: 10px; color: #909399; font-size: 12px; margin: 0 30px;">
      We use <a href="https://analytics.google.com">Google Analytics</a> to collect anonymous, aggregated usage information.
    </p>
  </footer>
</template>
<script setup lang="ts">
import { computed } from 'vue';
import { usePageData } from '@vuepress/client';

const pageData = usePageData();

const year = computed(() => new Date().getFullYear());

const getDocVersion = (branch = 'master', path = '') => {
  if (path.indexOf('UserGuide/Master') > -1 || path.indexOf('UserGuide') === -1) {
    return branch;
  }
  const branchRex = /UserGuide\/V(\d+\.\d+\.x)/;
  if (branchRex.test(path)) {
    const tag = branchRex.exec(path)![1];
    return `rel/${tag.replace('.x', '')}`;
  }
  return branch;
};

const docVersion = computed(() => getDocVersion('master', pageData.value.path));
</script>
