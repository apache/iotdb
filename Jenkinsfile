#!groovy

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

pipeline {

    agent {
        node {
            label 'ubuntu'
        }
    }

    environment {
        // Testfails will be handled by the jenkins junit steps and mark the build as unstable.
        MVN_TEST_FAIL_IGNORE = '-Dmaven.test.failure.ignore=true'
    }

    tools {
        maven 'maven_3_latest'
        jdk 'jdk_11_latest'
    }

    options {
        timeout(time: 4, unit: 'HOURS')
        // When we have test-fails e.g. we don't need to run the remaining steps
        skipStagesAfterUnstable()
    }

    stages {
        stage('Initialization') {
            steps {
                echo 'Building Branch: ' + env.BRANCH_NAME
                echo 'Using PATH = ' + env.PATH
            }
        }

        stage('Checkout') {
            steps {
                echo 'Checking out branch ' + env.BRANCH_NAME
                checkout scm
            }
        }

//         stage('Deploy site') {
//             when {
//                 branch 'master'
//             }
//             // Only the nodes labeled 'git-websites' have the credentials to commit to the.
//             agent {
//                 node {
//                     label 'git-websites'
//                 }
//             }
//             steps {
//                 // Publish the site with the scm-publish plugin.
//                 sh 'mvn -P site -P compile-site -P compile-site-1.0 -P compile-site-0.13 -P compile-site-0.12 -P compile-site-0.11 -P compile-site-0.10 -P compile-site-0.9 -P compile-site-0.8 compile scm-publish:publish-scm -pl site'
//
//                 // Clean up the snapshots directory (freeing up more space after deploying).
//                 dir("target") {
//                     deleteDir()
//                 }
//             }
//         }

        stage('Build and UT') {
            when {
                expression {
                    env.BRANCH_NAME ==~ "(master)|(rel/.*) |(jenkins-.*)"
                }
            }
            steps {
                echo 'Building and Unit Test...'
                sh "mvn ${MVN_TEST_FAIL_IGNORE} clean install -pl '!integration-test' -DskipITs"
            }
            post {
                always {
                    junit(testResults: '**/surefire-reports/*.xml', allowEmptyResults: true)
                    junit(testResults: '**/failsafe-reports/*.xml', allowEmptyResults: true)
                }
            }
        }

        stage('Integration Test') {
            when {
                expression {
                    env.BRANCH_NAME ==~ "(master)|(rel/.*) |(jenkins-.*)"
                }
            }
            steps {
                echo 'Integration Test...'
                sh "mvn ${MVN_TEST_FAIL_IGNORE} verify -P ClusterIT -pl integration-test -am -DskipUTs -DintegrationTest.threadCount=3 -DintegrationTest.forkCount=3"
            }
            post {
                always {
                    junit(testResults: '**/surefire-reports/*.xml', allowEmptyResults: true)
                    junit(testResults: '**/failsafe-reports/*.xml', allowEmptyResults: true)
                }
            }
        }

        stage('Deploy Prepare') {
        //             when {
        //                 branch 'master'
        //             }
            when {
                expression {
                    env.BRANCH_NAME ==~ "(master)|(rel/.*)"
                }
            }
            steps {
                echo 'Deploy Prepare'
                // We'll deploy to a relative directory so we can
                // deploy new versions only if the entire build succeeds
                sh "mvn -T 1C -DaltDeploymentRepository=snapshot-repo::default::file:./local-snapshots-dir clean deploy -DskipTests"
            }
            post {
                always {
                    junit(testResults: '**/surefire-reports/*.xml', allowEmptyResults: true)
                    junit(testResults: '**/failsafe-reports/*.xml', allowEmptyResults: true)
                }
            }
        }

        stage('Code Quality') {
            when {
                branch 'master'
            }
            steps {
                echo 'Checking Code Quality on SonarCloud'
                // Main parameters
                script {
                    // Then run the analysis
                    // 'my-sonarcloud-token' needs to be defined for this job and contains the user token
                    withCredentials([string(credentialsId: 'xiangdong-iotdb-sonarcloud-token', variable: 'SONAR_TOKEN')]) {
                        sh "mvn verify sonar:sonar -Dsonar.branch.name=master -Dsonar.host.url=https://sonarcloud.io -Dsonar.organization=apache -Dsonar.projectKey=apache_incubator-iotdb -Dsonar.login=${SONAR_TOKEN} -DskipTests"
                    }
                }
            }
        }


        stage('Deploy') {
            when {
                branch 'master'
            }
            steps {
                echo 'Deploying'
                // Deploy the artifacts using the wagon-maven-plugin.
                sh 'mvn -f jenkins.pom -X -P deploy-snapshots wagon:upload -P get-jar-with-dependencies'
            }
        }

        stage('Cleanup') {
            steps {
                echo 'Cleaning up the workspace'
                deleteDir()
            }
        }
    }

    // Send out notifications on unsuccessful builds.
    post {
        // If this build failed, send an email to the list.
        failure {
            script {
                if(env.BRANCH_NAME == "master") {
                    emailext(
                        subject: "[BUILD-FAILURE]: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]'",
                        body: """
BUILD-FAILURE: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]':

Check console output at "<a href="${env.BUILD_URL}">${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]</a>"
""",
                        to: "dev@iotdb.apache.org"
                    )
                }
            }
        }

        // If this build didn't fail, but there were failing tests, send an email to the list.
        unstable {
            script {
                if(env.BRANCH_NAME == "master") {
                    emailext(
                        subject: "[BUILD-UNSTABLE]: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]'",
                        body: """
BUILD-UNSTABLE: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]':

Check console output at "<a href="${env.BUILD_URL}">${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]</a>"
""",
                        to: "dev@iotdb.apache.org"
                    )
                }
            }
        }

        // Send an email, if the last build was not successful and this one is.
        success {
            script {
                if ((env.BRANCH_NAME == "master") && (currentBuild.previousBuild != null) && (currentBuild.previousBuild.result != 'SUCCESS')) {
                    emailext (
                        subject: "[BUILD-STABLE]: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]'",
                        body: """
BUILD-STABLE: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]':

Is back to normal.
""",
                        to: "dev@iotdb.apache.org"
                    )
                }
            }
        }
    }

}