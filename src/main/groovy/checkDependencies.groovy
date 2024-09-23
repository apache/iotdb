package src.main.groovy
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

var referenceFile = new File(basedir, "dependencies.txt")
if(!referenceFile.exists()) {
    throw new RuntimeException("Missing Reference: dependencies.txt")
}
var curBuildFile = new File(project.build.directory, "apache-${project.artifactId}-${project.version}-sbom.txt")
if(!curBuildFile.exists()) {
    throw new RuntimeException("Missing Build: apache-${project.artifactId}-${project.version}-sbom.txt")
}

// Simply compare the content of the two files for equality.
// TODO: Possibly a detailed diff output on the screen might be better, but for now this should do.
String referenceText = referenceFile.text
String buildText = curBuildFile.text
if(!referenceText.equals(buildText)) {
    throw new RuntimeException("The content of ${referenceFile.getPath()} and ${curBuildFile.getPath()} do not match")
}
