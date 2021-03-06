#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

readonly PROJECT_DIR=$(
  cd $(dirname "$0")
  pwd
)
readonly TARGET_DIR=${PROJECT_DIR}/target
readonly PROFILE=${1}

cd ${PROJECT_DIR}

mvn -B -ntp -q clean license:check install checkstyle:check spotbugs:check -P${PROFILE}
