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

import groovy.toml.TomlSlurper

def currentMavenVersion = project.version as String
def currentPythonVersion = currentMavenVersion
if(currentMavenVersion.contains("-SNAPSHOT")) {
    currentPythonVersion = currentMavenVersion.split("-SNAPSHOT")[0] + ".dev"
}
println "Current Maven Version:  " + currentMavenVersion
println "Current Python Version: " + currentPythonVersion

def pyprojectFile = new File(project.basedir, "pyproject.toml")
def ts = new TomlSlurper()
def toml = ts.parse(pyprojectFile)
def pyprojectFileVersion = toml.tool.poetry.version
if (pyprojectFileVersion != currentPythonVersion) {
    pyprojectFile.text = pyprojectFile.text.replace("version = \"" + pyprojectFileVersion + "\"", "version = \"" + currentPythonVersion + "\"")
    println "Version in pyproject.toml updated from " + pyprojectFileVersion + " to " + currentPythonVersion
    // TODO: When releasing, we might need to manually add this file to the release preparation commit.
} else {
    println "Version in pyproject.toml is up to date"
}