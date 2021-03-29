#!/usr/bin/env bash

set -e

BINDIR=`dirname "$0"`
SCRIPTS_DIR=`cd ${BINDIR}/;pwd`

source ${BINDIR}/common.sh

release_help() {
    cat <<EOF
Usage: release.sh <github-url> (major|minor|patch) <profiles>
EOF
}

# if no args specified, show usage
if [[ $# -lt 2 ]]; then
    release_help;
    exit 1;
fi

# get arguments
PROJECT_NAME=$1
shift

RELEASE_TYPE=$1
MAVEN_PROFILE=$2
case ${RELEASE_TYPE} in
    (major)
        ;;
    (minor)
        ;;
    (patch)
        ;;
    (fix)
        ;;
    (*)
        echo "Error: unknown release type ${RELEASE_TYPE}"
        release_help
        exit 1
        ;;
esac

GIVE_BRANCH="false"
if [ "x${3}" != "x" ]; then
    BRANCH_NAME=${3}
    GIVE_BRANCH="true"
fi

if [ "x${SNBOT_USER}" = "x" ]; then
    GITHUB_CREDENTIALS=""
else
    GITHUB_CREDENTIALS="${SNBOT_USER}:${SNBOT_PASSWORD}@"
fi
GITHUB_URL="https://${GITHUB_CREDENTIALS}github.com/streamnative/${PROJECT_NAME}.git"
PROJECT_NAME=${GITHUB_URL##*/}
MAVEN_SETTINGS_FILE="${SCRIPTS_DIR}/settings.xml"

# prepare a working space
WS_DIR="/tmp/release-${PROJECT_NAME}"
if [ -d ${WS_DIR} ]; then
    rm -rf ${WS_DIR}
fi
mkdir -p ${WS_DIR}
cd ${WS_DIR}

# configuring git
git config --global user.email "snbot@streamnative.io"
git config --global user.name "StreamNative Bot"

# cloning the repo
${GIT} clone ${GITHUB_URL} ${PROJECT_NAME}

if [ "x${GIVE_BRANCH}" = "xtrue" ]; then
    pushd ${PROJECT_NAME}
    git checkout ${BRANCH_NAME}
    popd
fi

# cd project dir and parse versions
#VERSION=`cd ${PROJECT_NAME} && mvn clean -DskipTests -q && mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -Ev '(^\[|Download\w+:)' | sed 's/^\(.*\)-SNAPSHOT/\1/'`
VERSION=`cd ${PROJECT_NAME} && mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec | sed 's/^\(.*\)-SNAPSHOT/\1/'`
VERSIONS_LIST=(`echo ${VERSION} | tr '.' ' '`)
_MAJOR_VERSION=${VERSIONS_LIST[0]}
_MINOR_VERSION=${VERSIONS_LIST[1]}
_PATCH_VERSION=${VERSIONS_LIST[2]}
_FIX_VERSION=${VERSIONS_LIST[3]:-1}

case ${RELEASE_TYPE} in
    (major)
        NEXT_MAJOR_VERSION=$((_MAJOR_VERSION + 1))
        NEXT_MINOR_VERSION=0
        NEXT_PATCH_VERSION=0
        ;;
    (minor)
        NEXT_MAJOR_VERSION=${_MAJOR_VERSION}
        NEXT_MINOR_VERSION=$((_MINOR_VERSION + 1))
        NEXT_PATCH_VERSION=0
        ;;
    (patch)
        NEXT_MAJOR_VERSION=${_MAJOR_VERSION}
        NEXT_MINOR_VERSION=${_MINOR_VERSION}
        NEXT_PATCH_VERSION=$((_PATCH_VERSION + 1))
        ;;
    (fix)
        NEXT_MAJOR_VERSION=${_MAJOR_VERSION}
        NEXT_MINOR_VERSION=${_MINOR_VERSION}
        NEXT_PATCH_VERSION=$((_PATCH_VERSION))
        NEXT_FIX_VERSION=$((_FIX_VERSION + 1))
esac

if [ "x${GIVE_BRANCH}" = "xfalse" ]; then
    CUR_VERSION="${_MAJOR_VERSION}.${_MINOR_VERSION}.${_PATCH_VERSION}"
    NEXT_VERSION="${NEXT_MAJOR_VERSION}.${NEXT_MINOR_VERSION}.${NEXT_PATCH_VERSION}"
    BRANCH_NAME="branch-${CUR_VERSION}"
    DEV_VERSION="${NEXT_VERSION}-SNAPSHOT"
    TAG="release-${CUR_VERSION}"
else
    CUR_VERSION="${_MAJOR_VERSION}.${_MINOR_VERSION}.${_PATCH_VERSION}.${_FIX_VERSION}"
    NEXT_VERSION="${NEXT_MAJOR_VERSION}.${NEXT_MINOR_VERSION}.${NEXT_PATCH_VERSION}.${NEXT_FIX_VERSION}"
    DEV_VERSION="${NEXT_VERSION}-SNAPSHOT"
    TAG="release-${CUR_VERSION}"
    VERSION=${CUR_VERSION}
fi

echo "-----------------------------------------------------"
echo "Releasing ${PROJECT_NAME} - ${VERSION}"
echo ""
echo "VERSION                   = ${VERSION}"
echo "NEXT_VERSION              = ${NEXT_VERSION}"
echo "DEV_VERSION               = ${DEV_VERSION}"
echo "BRANCH_NAME               = ${BRANCH_NAME}"
echo "TAG                       = ${TAG}"
echo "-----------------------------------------------------"
echo ""

# create the branch
echo "Creating release branch ${BRANCH_NAME} at ${PROJECT_NAME}"
echo "-----------------------------------------------------"

cd ${PROJECT_NAME}

if [ "x${GIVE_BRANCH}" = "xfalse" ]; then
    mvn --settings ${MAVEN_SETTINGS_FILE} \
        release:branch \
        -DbranchName=${BRANCH_NAME} \
        -DdevelopmentVersion=${DEV_VERSION} -P${MAVEN_PROFILE}
    # checkout the branch
    git checkout ${BRANCH_NAME}
fi


# prepare release
mvn --settings ${MAVEN_SETTINGS_FILE} \
    release:prepare \
    -DreleaseVersion=${VERSION} \
    -Dtag=${TAG} \
    -DupdateWorkingCopyVersions=false \
    -Darguments="-Dmaven.javadoc.skip=true -DskipTests=true" -P${MAVEN_PROFILE}


# build release
mvn --settings ${MAVEN_SETTINGS_FILE} \
    release:perform \
    -Darguments="-Dmaven.javadoc.skip=true -DskipTests=true" -P${MAVEN_PROFILE}

# publish bintray
curl --retry ${RETRY_TIMES:-10} -X POST \
    -ujaycroaker:${SNBOT_BINTRAY_PASSWORD} \
    https://api.bintray.com/content/streamnative/maven/io.streamnative.${PROJECT_NAME}/${VERSION}/publish

if [ "x${GIVE_BRANCH}" = "xtrue" ]; then
    mvn --settings ${MAVEN_SETTINGS_FILE} \
        --batch-mode release:update-versions \
        -DdevelopmentVersion=${DEV_VERSION}
    git add .
    git commit -m "Bump version to ${DEV_VERSION}"
    git push origin ${BRANCH_NAME}
fi
# clean up the workspace
rm -rf ${WS_DIR}