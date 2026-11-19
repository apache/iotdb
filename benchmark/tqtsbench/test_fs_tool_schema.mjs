/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// Compile against the installed DSH DSL without making any model request.
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { apply } from './fs-discovery-tools.mjs';
const toolsModule=pathToFileURL(process.argv[2]).href;
for (const features of [[], ['temporal','extreme'], ['grid'], ['reduce'], ['temporal','extreme','grid','reduce']]) {
  const registered=[];
  await apply({tools:{register:tool=>registered.push(tool)}},{features,toolsModule});
  assert.equal(registered.length,2);
}
console.log('Five active feature configurations compile with the real DSH schema compiler; no model calls.');
