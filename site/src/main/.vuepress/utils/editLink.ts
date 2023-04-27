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

import {
  isLinkHttp,
  removeEndingSlash,
  removeLeadingSlash,
} from '@vuepress/shared';
import { type RepoType, resolveRepoType } from 'vuepress-shared/client';

export const editLinkPatterns: Record<Exclude<RepoType, null>, string> = {
  GitHub: ':repo/edit/:branch/:path',
  GitLab: ':repo/-/edit/:branch/:path',
  Gitee: ':repo/edit/:branch/:path',
  Bitbucket:
    ':repo/src/:branch/:path?mode=edit&spa=0&at=:branch&fileviewer=file-view-default',
};

interface EditLinkOptions {
  docsRepo: string;
  docsBranch: string;
  docsDir: string;
  filePathRelative: string | null;
  editLinkPattern?: string;
}

const getBranch = (branch = 'master', path = '') => {
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
const getPath = (path: string) => {
  if (path.indexOf('UserGuide/Master') > -1 || path.indexOf('UserGuide') === -1) {
    return path.replace('UserGuide/Master', 'UserGuide');
  }
  const branchRex = /UserGuide\/V(\d+\.\d+\.x)/;
  if (branchRex.test(path)) {
    const tag = branchRex.exec(path)![1];
    return path.replace(`UserGuide/V${tag}`, 'UserGuide');
  }
  return path;
};

export const resolveEditLink = ({
  docsRepo,
  docsBranch,
  docsDir,
  filePathRelative,
  editLinkPattern,
}: EditLinkOptions): string | null => {
  if (!filePathRelative) return null;

  const repoType = resolveRepoType(docsRepo);

  let pattern: string | undefined;

  if (editLinkPattern) pattern = editLinkPattern;
  else if (repoType !== null) pattern = editLinkPatterns[repoType];

  if (!pattern) return null;

  return pattern
    .replace(
      /:repo/,
      isLinkHttp(docsRepo) ? docsRepo : `https://github.com/${docsRepo}`,
    )
    .replace(/:branch/, getBranch(docsBranch, filePathRelative))
    .replace(
      /:path/,
      removeLeadingSlash(`${removeEndingSlash(docsDir)}/${getPath(filePathRelative)}`),
    );
};
