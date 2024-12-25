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

import java.util.regex.Matcher

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// The entire Python "check" block is borrowed from Apache PLC4X's build.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

allConditionsMet = true

/**
 * Version extraction function/macro. It looks for occurrence of x.y or x.y.z
 * in passed input text (likely output from `program --version` command if found).
 *
 * @param input
 * @return
 */
private static Matcher extractVersion(input) {
    def matcher = input =~ /(\d+\.\d+(\.\d+)?).*/
    matcher
}

def checkVersionAtLeast(String current, String minimum) {
    def currentSegments = current.tokenize('.')
    def minimumSegments = minimum.tokenize('.')
    def numSegments = Math.min(currentSegments.size(), minimumSegments.size())
    for (int i = 0; i < numSegments; ++i) {
        def currentSegment = currentSegments[i].toInteger()
        def minimumSegment = minimumSegments[i].toInteger()
        if (currentSegment < minimumSegment) {
            println current.padRight(14) + " FAILED (required min " + minimum + " but got " + current + ")"
            return false
        } else if (currentSegment > minimumSegment) {
            println current.padRight(14) + " OK"
            return true
        }
    }
    def curNotShorter = currentSegments.size() >= minimumSegments.size()
    if (curNotShorter) {
        println current.padRight(14) + " OK"
    } else {
        println current.padRight(14) + " (required min " + minimum + " but got " + current + ")"
    }
    curNotShorter
}

def checkVersionAtMost(String current, String maximum) {
    def currentSegments = current.tokenize('.')
    def maximumSegments = maximum.tokenize('.')
    def numSegments = Math.min(currentSegments.size(), maximumSegments.size())
    for (int i = 0; i < numSegments; ++i) {
        def currentSegment = currentSegments[i].toInteger()
        def maximumSegment = maximumSegments[i].toInteger()
        if (currentSegment > maximumSegment) {
            println current.padRight(14) + " FAILED (required max " + maximum + " but got " + current + ")"
            return false
        } else if (currentSegment < maximumSegment) {
            println current.padRight(14) + " OK"
            return true
        }
    }
    def curNotShorter = currentSegments.size() >= maximumSegments.size()
    if (curNotShorter) {
        println current.padRight(14) + " OK"
    } else {
        println current.padRight(14) + " (required max " + maximum + " but got " + current + ")"
    }
    curNotShorter
}

def checkPython() {
    String python = project.properties['python.exe.bin']
    println "Using python executable:   " + python.padRight(14) + " OK"
    print "Detecting Python version:  "
    try {
        def process = (python + " --version").execute()
        def stdOut = new StringBuilder()
        def stdErr = new StringBuilder()
        process.waitForProcessOutput(stdOut, stdErr)
        Matcher matcher = extractVersion(stdOut + stdErr)
        if (matcher.size() > 0) {
            String curVersion = matcher[0][1]
            def result = checkVersionAtLeast(curVersion, "3.8.0")
            if (!result) {
                allConditionsMet = false
            }
            result = checkVersionAtMost(curVersion, "3.13")
            if (!result) {
                allConditionsMet = false
            }
        } else {
            println "missing (Please install at least version 3.8.0 and at most one of the 3.13.x versions)"
            allConditionsMet = false
        }
    } catch (Exception ignored) {
        println "missing"
        println "--- output of version `${python} --version` command ---"
        println output
        println "----------------------------------------------------"
        allConditionsMet = false
    }
}


// On Ubuntu it seems that venv is generally available, but the 'ensurepip' command fails.
// In this case we need to install the python3-venv package. Unfortunately checking the
// venv is successful in this case, so we need this slightly odd test.
def checkPythonVenv() {
    print "Detecting venv:            "
    try {
        def python = project.properties['python.exe.bin']
        def cmdArray = [python, "-Im", "ensurepip"]
        def process = cmdArray.execute()
        def stdOut = new StringBuilder()
        def stdErr = new StringBuilder()
        process.waitForProcessOutput(stdOut, stdErr)
        if (stdErr.contains("No module named")) {
            println "missing"
            println "--- output of version `python -Im \"ensurepip\"` command ---"
            println output
            println "------------------------------------------------------------"
            allConditionsMet = false
        } else {
            println "               OK"
        }
    } catch (Exception e) {
        println "missing"
        println "--- failed with exception ---"
        println e
        e.printStackTrace()
        println "----------------------------------------------------"
        allConditionsMet = false
    }
}

// Check the python environment is setup correctly.
checkPython()
checkPythonVenv()

if (!allConditionsMet) {
    throw new RuntimeException("Not all conditions met, see log for details.")
}
println ""
println "All known conditions met successfully."
println ""

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Calculate the version that we should use in the python build.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

def currentMavenVersion = project.version as String
def currentPythonVersion = currentMavenVersion
if(currentMavenVersion.contains("-SNAPSHOT")) {
    currentPythonVersion = currentMavenVersion.split("-SNAPSHOT")[0] + ".dev"
}
println "Current Project Version in Maven:  " + currentMavenVersion
println "Current Project Version in Python: " + currentPythonVersion

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Synchronize the version in pyproject.toml and the one used in the maven pom.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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