import { defineClientConfig } from '@vuepress/client';
import { Docsearch } from './components/index';

export default defineClientConfig({
  enhance({ app }) {
    app.component('Docsearch', Docsearch);
  },
});
