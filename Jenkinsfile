#!groovy

node {
    def MilevaDB_TEST_BRANCH = "master"
    def MilevaDB_BRANCH = "master"
    def FIDel_BRANCH = "master"

    fileLoader.withGit('git@github.com:whtcorpsinc/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/whtcorpsinc_einsteindb_branch.groovy').call(MilevaDB_TEST_BRANCH, MilevaDB_BRANCH, FIDel_BRANCH)
    }
}
