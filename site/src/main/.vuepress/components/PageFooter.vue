<template>
  <footer style="padding-bottom: 2rem;">
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

const year = computed(() => new Date().getFullYear());
</script>
