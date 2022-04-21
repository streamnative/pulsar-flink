#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# Enable the transaction in standalone config.
sed -i 's/transactionCoordinatorEnabled=false/transactionCoordinatorEnabled=true/g' /pulsar/conf/standalone.conf
sed -i 's/acknowledgmentAtBatchIndexLevelEnabled=false/acknowledgmentAtBatchIndexLevelEnabled=true/g' /pulsar/conf/standalone.conf
sed -i 's/systemTopicEnabled=false/systemTopicEnabled=true/g' /pulsar/conf/standalone.conf
sed -i 's/brokerDeduplicationEnabled=false/brokerDeduplicationEnabled=true/g' /pulsar/conf/standalone.conf

# Add auth configurations
sed -i 's/authenticationEnabled=false/authenticationEnabled=true/g' /pulsar/conf/standalone.conf
sed -i 's/authenticationProviders=/authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken/g' /pulsar/conf/standalone.conf
sed -i 's/authorizationEnabled=false/authorizationEnabled=true/g' /pulsar/conf/standalone.conf

sed -i 's/superUserRoles=/superUserRoles=user1/g' /pulsar/conf/standalone.conf
sed -i 's/brokerClientAuthenticationPlugin=/brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken/g' /pulsar/conf/standalone.conf
sed -i 's/brokerClientAuthenticationParameters=/brokerClientAuthenticationParameters=token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.2AgtxHe8-2QBV529B5DrRtpuqP6RJjrk21Mhnomfivo/g' /pulsar/conf/standalone.conf

sed -i 's$tokenSecretKey=$tokenSecretKey=data:base64,duaQk8phAAWZH5ohiZV92EE2/qqB3u//XHqE2T3BP10=$g' /pulsar/conf/standalone.conf


# Start Pulsar standalone without function worker and streaming storage.
/pulsar/bin/pulsar standalone --no-functions-worker -nss
