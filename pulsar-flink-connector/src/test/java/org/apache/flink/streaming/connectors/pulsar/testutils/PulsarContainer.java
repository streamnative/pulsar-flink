/*
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

package org.apache.flink.streaming.connectors.pulsar.testutils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

public class PulsarContainer extends GenericContainer<PulsarContainer> {
    public static final int BROKER_PORT = 6650;
    public static final int BROKER_HTTP_PORT = 8080;
    public static final String METRICS_ENDPOINT = "/metrics";
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("apachepulsar/pulsar");
    /**
     * @deprecated
     */
    @Deprecated
    private static final String DEFAULT_TAG = "2.7.1";
    private boolean functionsWorkerEnabled;

    /**
     * @deprecated
     */
    @Deprecated
    public PulsarContainer() {
        this(DEFAULT_IMAGE_NAME.withTag("2.7.1"));
    }

    /**
     * @deprecated
     */
    @Deprecated
    public PulsarContainer(String pulsarVersion) {
        this(DEFAULT_IMAGE_NAME.withTag(pulsarVersion));
    }

    public PulsarContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        this.functionsWorkerEnabled = false;
        this.withExposedPorts(new Integer[]{6650, 8080});
        this.withCommand(new String[]{"/pulsar/bin/pulsar", "standalone", "--no-functions-worker", "-nss"});
        this.waitingFor(Wait.forHttp("/metrics").forStatusCode(200).forPort(8080));
    }

    protected void configure() {
        super.configure();
        if (this.functionsWorkerEnabled) {
            this.withCommand(new String[]{"/pulsar/bin/pulsar", "standalone"});
            this.waitingFor((new WaitAllStrategy()).withStrategy(this.waitStrategy).withStrategy(Wait.forLogMessage(".*Function worker service started.*", 1)));
        }

    }

    public PulsarContainer withFunctionsWorker() {
        this.functionsWorkerEnabled = true;
        return this;
    }

    public String getPulsarBrokerUrl() {
        return String.format("pulsar://%s:%s", this.getHost(), this.getMappedPort(6650));
    }

    public String getHttpServiceUrl() {
        return String.format("http://%s:%s", this.getHost(), this.getMappedPort(8080));
    }
}
