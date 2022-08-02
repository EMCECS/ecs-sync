/*
 * Jenkinsfile for building ecs-sync for ECS
 */

import groovy.json.JsonOutput

// Load DRP integration library
@Library(['techops-global-lib']) _
DRPKafkaTopic = 'ecs-jenkins-events'

PIPELINES_BRANCH_NAME = 'master'

loader.loadFrom(['pipelines': [common          : 'common',
                               custom_packaging: 'packaging/custom_packaging'],
                 'branch'   : PIPELINES_BRANCH_NAME])

this.build()

void build() {
    Map<String, Object> args = [
        // object service
        branchName: params.BRANCH,
        version          : '',
        commit           : '',
        pushToArtifactory: params.PUSH_TO_HARBOR,
        publishRepoName  : common.ARTIFACTORY.ECS_BUILD_REPO_NAME,
        // slack - TODO: need to add integration for #ecs-sync-ci
        //slackChannel     : common.SLACK_CHANNEL.ECS_FLEX_CI,
    ]

    def scmData
    try {
        common.node(label: common.JENKINS_LABELS.FLEX_CI, time: 30) {
            /*
             * IMPORTANT: all sh() commands must be performed from withDockerContainer() block
             */
            common.withInfraDevkitContainer() {
                stage('Git Clone') {
                    scmData = checkout([
                          $class: 'GitSCM',
                          branches: scm.branches,
                          doGenerateSubmoduleConfigurations: false,
                          userRemoteConfigs: scm.userRemoteConfigs
                    ])
                    args.commit = scmData.GIT_COMMIT
                    reportBuildStart(script: this, topic: DRPKafkaTopic, scmData: scmData) // publish metrics to DRP
                }

                stage('Build Client') {
                    sh("./gradlew jar")
                }

                stage('Publish Client') {
                    if (args.pushToArtifactory == true) {
                        String publishUrl = common.ARTIFACTORY.getUrlForRepo(args.publishRepoName, true)
                        String publishCred = common.ARTIFACTORY.getCredentialsIdForPublish(args.publishRepoName)
                        timeout(60) {
                            withCredentials([[$class          : 'UsernamePasswordMultiBinding',
                                              credentialsId   : publishCred,
                                              usernameVariable: 'USERNAME',
                                              passwordVariable: 'PASSWORD',],]) {
                                sh("./gradlew -PpublishUrl=${publishUrl} -PpublishUsername=${env.USERNAME} -PpublishPassword=${env.PASSWORD} publish")
                            }
                        }
                    }
                }

                // TODO: define CI test job
                if (env.JOB_NAME.contains("ecs-sync-master-build")) {
                    stage('Run CI Job') {
                        // start ci job: ecs-sync-master-ci
                        build([job       : 'ecs-sync-master-ci'.toString(),
                               parameters: [
                                       string(name: 'ECS_SYNC_VERSION', value: args.version),
                                       string(name: 'AUTOMATION_REPO_BRANCH_NAME', value: args.branchName),
                               ],
                               wait      : false,
                               propagate : false,
                        ])
                    }
                } else {
                    print("Job ${env.JOB_NAME} not master build. Don't trigger objectsvc master CI")
                }
            }
        }
    }
    catch (any) {
        println any
        common.setBuildFailure()
        throw any
    }
    finally {
        currentBuild.description = common.getObjectServiceBuildDescription(args)
        //common.slackSend(channel: args.slackChannel, sendOnFailureOnly: true)
        reportBuildResult(script: this, topic: DRPKafkaTopic, scmData: scmData) // publish metrics to DRP
    }
}

this