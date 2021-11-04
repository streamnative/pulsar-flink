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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_SINGLE_OPTION_KEY;
import static org.junit.Assert.assertTrue;

public class SinkSrcRoundtripTest extends PulsarTestBase {

    private static final String TOPIC_UNENCRYPTED = TopicName.get("persistent", "public", "default", "unencrypted-topic").toString();
    private static final String TOPIC_ENCRYPTED = TopicName.get("persistent", "public", "default", "encrypted-topic").toString();

    @Test(timeout = 60 * 1000)
    public void testUnencryptedRoundtrip() throws Exception {
        final Properties properties = new Properties();
        final String topic = TOPIC_UNENCRYPTED;
        properties.put(TOPIC_SINGLE_OPTION_KEY, topic);
        final SimpleStringSchema schema = new SimpleStringSchema();
        final PulsarSerializationSchemaWrapper<String> serializationSchema = new PulsarSerializationSchemaWrapper.Builder<>(schema)
            .useSpecialMode(Schema.STRING)
            .build();

        final FlinkPulsarSink<String> sink = new FlinkPulsarSink.Builder<String>()
            .withAdminUrl(getAdminUrl())
            .withServiceUrl(getServiceUrl())
            .withDefaultTopicName(topic)
            .withProperties(properties)
            .withPulsarSerializationSchema(serializationSchema)
            .build();

        final FlinkPulsarSource<String> src = new  FlinkPulsarSource.Builder<String>()
            .withAdminUrl(getAdminUrl())
            .withServiceUrl(getServiceUrl())
            .withProperties(properties)
            .withDeserializionSchema(schema)
            .build()
            .setStartFromEarliest();

        roundtrip(sink, src);
    }

    private static class MyCryptoKeyReader implements CryptoKeyReader {

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
            EncryptionKeyInfo pub = new EncryptionKeyInfo();
            pub.setKey(("" +
                    "-----BEGIN RSA PUBLIC KEY-----\n" +
                    "MIICCgKCAgEAyuz24j5TMVi9PthJpGYQ0sGJ+5uKPoVeoQQPvagOlIhKMCpjitmO\n" +
                    "gZo5LvMFT0lACVzdxVkUpqEbRc9upopLDAhhHeHTauOxi7kH1iDO9kMKE5uy5QK5\n" +
                    "JveG56drwIXrvQyz9NmEIkmSG/Ruisl9zDr+t3cdyLdwE4T0YU8JCj1Ex3Pnlr8c\n" +
                    "iJoYV7VZoqJPJF3Le3PbXORxn5lXDnZcVkdDCDSLITAHegN5DU6/UJqIgwMQo0fQ\n" +
                    "2dMiZ/VzqiPF1RHrBoM7/P2u507k2ZOS4jrIbKj9YdV4AibTDEYIhtSyIPkEJGDJ\n" +
                    "GZINSxcfW9+exnq28B+NsJwwKNVoU60Phui6PcCOF4BXRYWMajJyLIrTNo8cFL5Z\n" +
                    "Kid1+5BrYxCBdWDHJ4KiaT/y8y9q8ea+kUrUgp3VbsDstN1gU3vxzPcvaiTZc5xK\n" +
                    "puEg4bbY5waN1y8JyfjwRUBk29CTXh3wQbd81DBjykfVL6OcbcVH8V0qLN7uhAPm\n" +
                    "EGFyyqlwH93HsSCTHjJpkxBj2gO8n7/5YQJe9181tz+0xofc9kpDT1YTxdHxJevA\n" +
                    "UsRfkrCGbwAdE2QhGmzwJSJCPdIanTEGRK8fr/6T0EM7TwrmHgLsCybpqdMil15u\n" +
                    "8crgr8N7wTfm/iikdVs1sOtkjfG5WoNwm8XqS7g4CrVBFIVu/v8o++ECAwEAAQ==\n" +
                    "-----END RSA PUBLIC KEY-----")
                    .getBytes());
            return pub;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
            EncryptionKeyInfo priv = new EncryptionKeyInfo();
            priv.setKey(("" +
                "-----BEGIN RSA PRIVATE KEY-----\n" +
                "MIIJKAIBAAKCAgEAyuz24j5TMVi9PthJpGYQ0sGJ+5uKPoVeoQQPvagOlIhKMCpj\n" +
                "itmOgZo5LvMFT0lACVzdxVkUpqEbRc9upopLDAhhHeHTauOxi7kH1iDO9kMKE5uy\n" +
                "5QK5JveG56drwIXrvQyz9NmEIkmSG/Ruisl9zDr+t3cdyLdwE4T0YU8JCj1Ex3Pn\n" +
                "lr8ciJoYV7VZoqJPJF3Le3PbXORxn5lXDnZcVkdDCDSLITAHegN5DU6/UJqIgwMQ\n" +
                "o0fQ2dMiZ/VzqiPF1RHrBoM7/P2u507k2ZOS4jrIbKj9YdV4AibTDEYIhtSyIPkE\n" +
                "JGDJGZINSxcfW9+exnq28B+NsJwwKNVoU60Phui6PcCOF4BXRYWMajJyLIrTNo8c\n" +
                "FL5ZKid1+5BrYxCBdWDHJ4KiaT/y8y9q8ea+kUrUgp3VbsDstN1gU3vxzPcvaiTZ\n" +
                "c5xKpuEg4bbY5waN1y8JyfjwRUBk29CTXh3wQbd81DBjykfVL6OcbcVH8V0qLN7u\n" +
                "hAPmEGFyyqlwH93HsSCTHjJpkxBj2gO8n7/5YQJe9181tz+0xofc9kpDT1YTxdHx\n" +
                "JevAUsRfkrCGbwAdE2QhGmzwJSJCPdIanTEGRK8fr/6T0EM7TwrmHgLsCybpqdMi\n" +
                "l15u8crgr8N7wTfm/iikdVs1sOtkjfG5WoNwm8XqS7g4CrVBFIVu/v8o++ECAwEA\n" +
                "AQKCAgBB5lCKypizxtC2bwEDXY4LE4Ue67Uqdp9zhOEjw0bw343QNIPdHKfV2OLH\n" +
                "J27K/8vG/pyasUIultVHh4S0muaiQrpfPO4uoUEQUgeEd2Uevkiwc3jWPFsql2n9\n" +
                "IvawMA2NeGmck2MAy4migG/BrIuo3mPH6uwGOeQwwpWmYEdcRudmKnLEFs5KYliT\n" +
                "azZvxWwUME2bitVrRljL7r1B2hhEgKH5MS8ZmQJkkmomczNYFsdMXJtzmyftBU8A\n" +
                "Gcr1LubZOhdsJwQ9NZkuTwWszur9gv+BoiOfOPbfJAKX0sqEFuC+KoA43CGSp0af\n" +
                "4yNw758db86nDmgyOZa+PAfEXMhUf/3VFbnkca8w9vAP5w8b0W8GtCflcnC+vb1y\n" +
                "qcToLbHaDnmhHLUdxHVj2XLTNxYFxmCmPhS+R8XRX4yPftni1skxjNOa6QbmHgVq\n" +
                "7DgXcJJxJBk5+EIJrujk/eXzfU2zwUsYMt1dFLzwAvdc2fyoe8CCiBN+VUrhGoTF\n" +
                "3IvYMzHYAdpxTdhKtQfShdI+tS3PLiEbzB3DxHH6H9X6Q0HZdLyGRufpqDW6DW1M\n" +
                "zzCJIXRIcj8oZk36JsoH00RGOsCjdaeDOL13a3LCn0yBjuRPRVD0UlJA3EZT4/Ic\n" +
                "QNvbgQe6zLbeIWWaYcstcAyjBCcW0mzEdlyUFIjU9LIVuJRhkQKCAQEA+D6p8KSb\n" +
                "LhkwDGrt8X60QxattrRGVir5o/7lGCHkcnNBQUNXXy/8hrS41qsRhDHPSYHZFLwV\n" +
                "66sIolkWhA9JEdZKfT/CsamHkfho8aitHh8YShdOog1OZQsunhFkaLzXO1mvDSUF\n" +
                "W5ZLogUWy4oAduVwb4ANzNuW0pMGTYSzQHNJoHb91c/iu755av3VxeuxmomaKqBB\n" +
                "h5TrNkHA+fUFvJqn/GZI0ZVyAqvIbyn7qN8y9rZcTa05L8oR2oyL7WVWF61ym03O\n" +
                "V3f1gjSOU12acAGx5T6B6Bofb0v1xGgb4NMjacZcJ/lur9VmDZMlDt6XG6zWNalS\n" +
                "/cB36JKQRgijfQKCAQEA0UPca7dG8cRryYpKYfHcCX3RIjp89XB83SWBpvNJr2+m\n" +
                "BBHlsHPyU/NAVrh7rCL4S+TMsp8Dl/6sRjcon5R0a1gJfHPjmLTVx9tAD5Jbmn14\n" +
                "zGemQDaGsV1DHxtL6B1a7qlKqq2QM4N6vzz18DvAkISs74iO9b+RwSFRLvkdmYQk\n" +
                "e2CXDedNIPQlABV4nJlIQD6dAFBSdibf2xC8goXfxEtcRK6wf9/2SR0SVeOxnKDi\n" +
                "KkeA9tqChW4Skk+WCHmDL8k375aYgViJcTmFD1cJHlj0lQGfk+gnxJnJwQJL7Cqg\n" +
                "UYJxrX667sNnga8dcwFjqLF/hzAb75d+8cTXAE8fNQKCAQBOwdK4fgCdh3AvAF2t\n" +
                "GD2oazGBnYATJl89IEkeduI7TUWOpwa5NEgxlHRv5qYQAp14/LEaWvG5avG6T/lM\n" +
                "vGy6M/o98lSaeOaB8QWaZaFGxSa3mt1fnEka1YlcrLfmYsMGGVXoHa6td+lW5bZt\n" +
                "rMKo9fHN7hpyu9gFxo9hWJBmCi15s0ak5udQGQX8Y7vGpxgZpz45983SbfSRqhrH\n" +
                "Mm03gPl6ohjIJVmeb1GPswoccXOBwilWm3ZhKwKvC5f5IQVHTcfmbbDhHzXMsU/W\n" +
                "MwQkNOVzjXk5YdBHRxoZzc3KbjH2BPCH3iK3tkRCWkSPix71sMflDms+BioEpzsO\n" +
                "fP8hAoIBAQDRIYdr4pq0zP6HSHvzjDjBB4r0MQ1mX8d5Xp1GkkYmXGbGFHi+MfGQ\n" +
                "Mj4vLGjz63LGrd5f+AgoYywZc9BWQo9iI3Y/eLWQi9BFzfgkV7jSGOibJk6AR72u\n" +
                "DS0iLi5axtN0RZ1IGvJMeO43ph2GusBD7UPCkm+EarGoF7rBPdZ18BhhcHMlQu3S\n" +
                "rAs6HTsPDSSmh6xxftQaHdmDXSN3MYEh88o/HXFoKhNAmBwV19pNVH8Rj6nziQX9\n" +
                "gLZwn7apu33+SJJtDsxUH34juD8gyHNlb7LmItwufUkY8jQtfjUPzL2xF7Kxl0AL\n" +
                "kx6i/LVqlI3bLZ/sI4kXlQgZaAUR2wCtAoIBAG/EIzsdMmM5ym7FX1KuQX1FsEFN\n" +
                "avzwjnpzR4dp3m8/IS2yTKxyb83zHauox+1m6PnEU8yihvJkAt4JJSk9aC4PeZt+\n" +
                "b3MV6lHGgN4Q5EjGGOwEDh4yfyCW6eJACyIc7eyySXA0W3dvnZChgp6zav/3rZr7\n" +
                "mETaPxWH9AYy2U9KLzSYNPrYVnPz97vjwFLRAsWQCRhYm/4mU/53GCqsxW7lW1f1\n" +
                "e6NqFGHNw6ubOl4p3lNX53mYp4Fn7YJNumAftEBd8r9LS7HYL7XUzYrrgt57WVMt\n" +
                "8svUxDU98U1YCSNWAupd4se08tdprO5czULIEjCqS1we4J07hICeWEaxfss=\n" +
                "-----END RSA PRIVATE KEY-----").getBytes());
            return priv;
        }
    }

    @Test(timeout = 60 * 1000)
    public void testEncryptedRoundtrip() throws Exception {

        final Properties properties = new Properties();
        final String topic = TOPIC_ENCRYPTED;
        properties.put(TOPIC_SINGLE_OPTION_KEY, topic);
        final SimpleStringSchema schema = new SimpleStringSchema();
        final PulsarSerializationSchemaWrapper<String> serializationSchema = new PulsarSerializationSchemaWrapper.Builder<>(schema)
            .useSpecialMode(Schema.STRING)
            .build();

        CryptoKeyReader cryptoKeyReader = new MyCryptoKeyReader();

        final FlinkPulsarSink<String> sink = new FlinkPulsarSink.Builder<String>()
            .withAdminUrl(getAdminUrl())
            .withServiceUrl(getServiceUrl())
            .withDefaultTopicName(topic)
            .withProperties(properties)
            .withPulsarSerializationSchema(serializationSchema)
            .withCryptoKeyReader(cryptoKeyReader)
            .withEncryptionKeys("key1", "key2")
            .build();

        final FlinkPulsarSource<String> src = new  FlinkPulsarSource.Builder<String>()
            .withAdminUrl(getAdminUrl())
            .withServiceUrl(getServiceUrl())
            .withProperties(properties)
            .withDeserializionSchema(schema)
            .withCryptoKeyReader(cryptoKeyReader)
            .build()
            .setStartFromEarliest();

        roundtrip(sink, src);
    }

    private void roundtrip(FlinkPulsarSink<String> sink, FlinkPulsarSource<String> src) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> input = new ArrayList<>();
        input.add("Hello");
        input.add("world");
        input.add("!");

        final DataStream<String> out = env
            .fromCollection(input);
        out.addSink(sink);

        final DataStream<String> in = env.addSource(src);
        in.addSink(new PrintSinkFunction<>());
        final Iterator<String> iterator = in.executeAndCollect("Roundtrip Job");

        final List<String> results = new LinkedList<>();
        while (iterator.hasNext()){
            final String item = iterator.next();
            results.add(item);
            assertTrue(input.containsAll(results));
            if (results.size() >= input.size()){
                // Happy!
                break;
            }
        }
    }

}
