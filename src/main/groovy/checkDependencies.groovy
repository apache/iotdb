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

import groovy.json.JsonSlurper

if(Boolean.parseBoolean(properties['skipDependencyCheck']).booleanValue()) {
    println "Skipping dependency check"
    return
}

def jsonSlurper = new JsonSlurper()

var referenceFile = new File(basedir, "dependencies.json")
if(!referenceFile.exists()) {
    throw new RuntimeException("Missing Reference: dependencies.json")
}
def referenceJson = jsonSlurper.parse(referenceFile)

var curBuildFile = new File(project.build.directory, "apache-${project.artifactId}-${project.version}-sbom.transformed.json")
if(!curBuildFile.exists()) {
    throw new RuntimeException("Missing Build: apache-${project.artifactId}-${project.version}-sbom.transformed.json")
}
def curBuildJson = jsonSlurper.parse(curBuildFile)

def differencesFound = false
referenceJson.dependencies.each {
    if(!curBuildJson.dependencies.contains(it)) {
        println "current build has removed a dependency: " + it
        differencesFound = true
    }
}
curBuildJson.dependencies.each {
    if(!referenceJson.dependencies.contains(it)) {
        println "current build has added a dependency: " + it
        differencesFound = true
    }
}

if(differencesFound) {
    println "Differences were found between the information in ${referenceFile.getPath()} and ${curBuildFile.toPath()}"
    println "The simplest fix for this, is to replace the content of ${referenceFile.getPath()} with that of ${curBuildFile.toPath()} and to inspect the diff of the resulting file in your IDE of choice."
    throw new RuntimeException("Differences found.")
}