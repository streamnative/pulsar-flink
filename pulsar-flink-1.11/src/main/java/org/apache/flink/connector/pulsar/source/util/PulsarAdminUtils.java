/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.util;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

/**
 * A util class to help with the use of pulsarAdmin.
 */
public class PulsarAdminUtils {

    public static PulsarAdmin newAdminFromConf(String adminUrl, ClientConfigurationData clientConfigurationData) throws PulsarClientException {
        ClientConfigurationData adminConf = clientConfigurationData.clone();
        adminConf.setServiceUrl(adminUrl);
        setAuth(adminConf);
        return new PulsarAdmin(adminUrl, adminConf);
    }

    private static void setAuth(ClientConfigurationData conf) throws PulsarClientException {
        if (!StringUtils.isBlank(conf.getAuthPluginClassName()) && !StringUtils.isBlank(conf.getAuthParams())) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParams()));
        }
    }
}
