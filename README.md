# Pulsar Flink Connector

The Pulsar Flink connector implements elastic data processing using [Apache Pulsar](https://pulsar.apache.org) and [Apache Flink](https://flink.apache.org).

For details about the Chinese document, see [here](doc/README_CN.md).

# Prerequisites

- Java 8 or higher version
- Flink 1.9.0 or higher version
- Pulsar 2.4.0 or higher version

# Basic information

This section describes basic information about the Pulsar Flink connector.

## Client

Currently, the following Flink versions are supported.

- Flink 1.9 - 1.10: they are maintained in the [`flink-1.9` branch](https://github.com/streamnative/pulsar-flink/tree/flink-1.9).

- Flink 1.11: it is maintained in the [`flink-1.11` branch](https://github.com/streamnative/pulsar-flink/tree/flink-1.11).

- Flink 1.12: it is maintained in the [`master` branch](https://github.com/streamnative/pulsar-flink/tree/master).

> **Note**  
> Since Flink's API has changed greatly, we mainly work on new features in the `master` branch and fix bugs in other branches.

The JAR package is located in the [Bintray Maven repository of StreamNative](https://dl.bintray.com/streamnative/maven).

For projects using SBT, Maven, or Gradle, you can set the following parameters for your project.

- `FLINK_VERSION`: currently, versions `1.9`, `1.11`, and `1.12` are available.
- `SCALA_BINARY_VERSION`: this parameter defines the Scala version used by Flink. Versions `2.11` and `2.12` are available.
- `PULSAR_FLINK_VERSION`: it is the version of the Pulsar Flink connector. Usually, use a three-digit version (such as version `2.7.0`) for a master release and a four-digit version for a branch release (such as version `2.7.0.1`).

Here is an example about how to configure parameters for projects using SBT, Maven, or Gradle.

```shell
groupId = io.streamnative.connectors
artifactId = pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{FLINK_VERSION}}
version = {{PULSAR_FLINK_VERSION}}
```

## Maven projects

For Maven projects, you can add the repository configuration to your `pom.xml`, as shown below.

```xml
  <repositories>
    <repository>
      <id>central</id>
      <layout>default</layout>
      <url>https://repo1.maven.org/maven2</url>
    </repository>
    <repository>
      <id>bintray-streamnative-maven</id>
      <name>bintray</name>
      <url>https://dl.bintray.com/streamnative/maven</url>
    </repository>
  </repositories>
```

For Maven projects, you can use the following [shade](https://imperceptiblethoughts.com/shadow/) plugin definition template to build an application JAR package that contains all the dependencies required for the client library and Pulsar Flink connector.

```xml
<plugin>
  <!-- Shade all the dependencies to avoid conflicts -->
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <version>${maven-shade-plugin.version}</version>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <createDependencyReducedPom>true</createDependencyReducedPom>
        <promoteTransitiveDependencies>true</promoteTransitiveDependencies>
        <minimizeJar>false</minimizeJar>

        <artifactSet>
          <includes>
            <include>io.streamnative.connectors:*</include>
            <!-- more libs to include here -->
          </includes>
        </artifactSet>
        <filters>
          <filter>
            <artifact>*:*</artifact>
            <excludes>
              <exclude>META-INF/*.SF</exclude>
              <exclude>META-INF/*.DSA</exclude>
              <exclude>META-INF/*.RSA</exclude>
            </excludes>
          </filter>
        </filters>
        <transformers>
          <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
          <transformer implementation="org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer" />
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```

## Gradle projects

For Gradle projects, you can add the repository configuration to your `build.gradle`, as shown below.

```groovy
repositories {
         maven {
             url 'https://dl.bintray.com/streamnative/maven'
         }
}
```

For gradle projects, you can use the following [shade](https://imperceptiblethoughts.com/shadow/) plugin definition template to build an application JAR package that contains all the dependencies required for the client library and Pulsar Flink connector.

```groovy
buildscript {
     repositories {
         jcenter()
     }
     dependencies {
         classpath 'com.github.jengelman.gradle.plugins:shadow:6.0.0'
     }
}

apply plugin: 'com.github.johnrengelman.shadow'
apply plugin: 'java'
```

# Build Pulsar Flink connector

To build the Pulsar Flink connector for reading data from Pulsar or writing the results to Pulsar, follow these steps.

1. Check out the source code.

  ```bash
  git clone https://github.com/streamnative/pulsar-flink.git
  cd pulsar-flink
  ```

2. Install the Docker.

  The Pulsar Flink connector uses [Testcontainers](https://www.testcontainers.org/) for integration test. To run the integration test, ensure to install the Docker. For details about how to install the Docker, see [here](https://docs.docker.com/docker-for-mac/install/).

3. Set the Java version.

   Modify `java.version` and `java.binary.version` in `pom.xml`.

   > **Note**  
   > Ensure that the Java version should be identical to the Java version for the Pulsar Flink connector.

4. Build the project.

  ```bash
  mvn clean install -DskipTests
  ```

5. Run the test.

  ```bash
  mvn clean install
  ```

After the Pulsar Flink connector is installed, a JAR package that contains all the dependencies is generated in both the local Maven repository and the `target` directory.

> **Note**  
> If you use intellij IDEA to debug this project, you might encounter the `org.apache.pulsar.shade.org.bookkeeper.ledger` package error. To fix the error, use the ` mvn clean install -DskipTests` command to install the JAR package to the local repository, ignore the `managed-ledger-shaded` Maven module on the project, and then click **Refresh**.

# Deploy Pulsar Flink connector

This section describes how to deploy the Pulsar Flink connector.

## Client library

For any Flink application, use the `./bin/flink run` command to compile and start your application.

If you have already built a JAR package with dependencies using the above shade plugin, you can use the `--classpath` option to add your JAR package.

> **Note**  
> The path must be in a protocol format (such as `file://`) and the path must be accessible on all nodes.

**Example**

```
./bin/flink run -c com.example.entry.point.ClassName file://path/to/jars/your_fat_jar.jar
```

## Scala REPL

The Scala REPL is a tool (scala) for evaluating expressions in Scala. Use the `bin/start-scala-shell.sh` command to deploy Pulsar Flink connector on Scala client. You can use the `--addclasspath` to add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{ PULSAR_FLINK_VERSION}}.jar` package.

**Example**

```
./bin/start-scala-shell.sh remote <hostname> <portnumber>
 --addclasspath pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```

For more information on submitting applications through the CLI, see [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/cli.html) .

## SQL client

The [SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sqlClient.html) is used to write SQL queries for manipulating data in Pulsar, you can use the `-addclasspath` option to add `pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar` package.

**Example**

```
./bin/sql-client.sh embedded --jar pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```

> **Note**  
> If you put the JAR package of our connector under `$FLINK_HOME/lib`, do not use `--jar` again to specify the package of the connector.

By default, to use the Pulsar directory in the SQL client and register it automatically at startup, the SQL client reads its configuration from the `./conf/sql-client-defaults.yaml` environment file. You need to add the Pulsar catalog to the `catalogs` section of this YAML file, as shown below.

```yaml
catalogs:
- name: pulsarcatalog
    type: pulsar
    default-database: tn/ns
    service-url: "pulsar://localhost:6650"
    admin-url: "http://localhost:8080"
    format: json
```

# Usage

This section describes how to use the Pulsar Flink connector in the stream environment and table environment.

## Stream environment

This section describes how to use the Pulsar Flink connector in the stream environment.

### Source

In Pulsar Flink, the Pulsar consumer is called `FlinkPulsarSource<T>`. It accesses to one or more Pulsar topics.

Its constructor method has the following parameters.

- `serviceUrl` (service address) and `adminUrl` (administrative address): they are used to connect to the Pulsar instance.
- `PulsarDeserializationSchema<T>`: when the `FlinkPulsarSource` is used, you need to set the `PulsarDeserializationSchema<T>` parameter.
- `Properties`: it is used to configure the behavior of the Pulsar consumer, including the `topic`, `topics`, and `topicsPattern` options. The `topic`, `topics`, or `topicsPattern` option is used to configure information about the topic to be consumed. You must set a value for it. (**The `topics` parameters refers to multiple topics separated by a comma (,), and the `topicsPattern` parameter is a Java regular expression that matches a number of topics**.)
- `setStartFromLatest`, `setStartFromEarliest`, `setStartFromSpecificOffsets`, or `setStartFromSubscription`: these parameters are used to configure the consumption mode. When the `setStartFromSubscription` consumption mode is configured, the checkpoint function must be enabled.

**Example**

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic");
props.setProperty("partition.discovery.interval-millis", "5000");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()), props);

// or setStartFromLatest、setStartFromSpecificOffsets、setStartFromSubscription
source.setStartFromEarliest(); 

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```

### Sink

The Pulsar producer uses the `FlinkPulsarSink` instance. It allows to write record streams to one or more Pulsar topics.

**Example**

```java
PulsarSerializationSchema<Person> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(Person.class))
.usePojoMode(Person. class, RecordSchemaType.JSON)
.setTopicExtractor(person -> null)
.build();
FlinkPulsarSink<Person> sink = new FlinkPulsarSink(
  serviceUrl,
  adminUrl,
  Optional.of(topic),      // mandatory target topic or use `Optional.empty()` if sink to different topics for each record
  props,
  pulsarSerialization,
  PulsarSinkSemantic.AT_LEAST_ONCE);

stream.addSink(sink);
```

### PulsarDeserializationSchema

PulsarDeserializationSchema is a connector-defined Flink DeserializationSchema wrapper that allows flexible manipulation of Pulsar messages.

PulsarDeserializationSchemaWrapper is a simple implementation of PulsarDeserializationSchema with two parameters: Flink DeserializationSchema and information about the decoded message type.

```
PulsarDeserializationSchemaWrapper(new SimpleStringSchema(),DataTypes.STRING())
```
> **Note**  
> The `DataTypes` type comes from Flink's `table-common` module.

### PulsarSerializationSchema

PulsarSerializationSchema is a wrapper for Flink SerializationSchema that provides more functionality. In most cases, users do not need to implement PulsarSerializationSchema by themselves. PulsarSerializationSchemaWrapper is provided to wrap a Flink SerializationSchema as  PulsarSerializationSchema.

PulsarSerializationSchema uses the builder pattern and you can call `setKeyExtractor` or `setTopicExtractor` to extract the key and customize the target topic from each message.

In particular, since Pulsar maintains its own Schema information internally, our messages must be able to export SchemaInfo when they are written to Pulsar. The `useSpecialMode`, `useAtomicMode`, `usePojoMode`, and `useRowMode` methods help you quickly build the Schema information required for Pulsar. You must choose one of these four modes.

- SpecialMode: specify the `Schema<?>` mode directly. Ensure that this Schema is compatible with the Flink SerializationSchema setting.
- AtomicMode: For some atomic types, pass the type of AtomicDataType, such as `DataTypes.INT()`, which corresponds to `Schema<Integer>` in Pulsar.
- PojoMode: you need to pass a custom class object and either JSON or Arvo Schema to specify how to build a composite type Schema, such as `usePojoMode(Person.class, RecordSchemaType.JSON)`.
- RowMode: in general, it is used for our internal `Table&SQL` API implementation.

### Fault tolerance

With Flink's checkpoints being enabled, `FlinkPulsarSink` can provide at-least-once and exactly-once delivery guarantees.

In addition to enabling checkpoints for Flink, you should also configure `setLogFailuresOnly(boolean)` and `setFlushOnCheckpoint(boolean)` parameters.

> **Note**  
> `setFlushOnCheckpoint(boolean)`: by default, it is set to `true`. When it is enabled, writing to Pulsar records is performed at this checkpoint snapshotState. This ensures that all records before the checkpoint are written to Pulsar. And, at-least-once setting must also be enabled.

## Table environment

The Pulsar Flink connector supports all the Table features, as listed below.

- SQL and DDL
- Catalog

### SQL and DDL

The following section describes SQL configurations and DDL configurations.

#### SQL configurations

```sql
CREATE TABLE pulsar (
  `physical_1` STRING,
  `physical_2` INT,
  `eventTime` TIMESTAMP(3) METADATA,
  `properties` MAP<STRING, STRING> METADATA ,
  `topic` STRING METADATA VIRTUAL,
  `sequenceId` BIGINT METADATA VIRTUAL,
  `key` STRING ,
  `physical_3` BOOLEAN
) WITH (
  'connector' = 'pulsar',
  'topic' = 'persistent://public/default/topic82547611',
  'key.format' = 'raw',
  'key.fields' = 'key',
  'value.format' = 'avro',
  'service-url' = 'pulsar://localhost:6650',
  'admin-url' = 'http://localhost:8080',
  'scan.startup.mode' = 'earliest' 
)

INSERT INTO pulsar 
VALUES
 ('data 1', 1, TIMESTAMP '2020-03-08 13:12:11.123', MAP['k11', 'v11', 'k12', 'v12'], 'key1', TRUE),
 ('data 2', 2, TIMESTAMP '2020-03-09 13:12:11.123', MAP['k21', 'v21', 'k22', 'v22'], 'key2', FALSE),
 ('data 3', 3, TIMESTAMP '2020-03-10 13:12:11.123', MAP['k31', 'v31', 'k32', 'v32'], 'key3', TRUE)
 
SELECT * FROM pulsar
```

SQL supports configuring physical fields, calculated columns, watermark, METADATA and other features.

#### DDL configurations

| Parameter                    | Default value | Description                                                  | Required or not |
| ----------------------------- | ------------- | ------------------------------------------------------------ | -------- |
| connector                     | null          | Set the connector type. Available options are `pulsar` and `upsert-pulsar`. | Yes      |
| topic                         | null          | Set the input or output topic, use half comma for multiple and concatenate topics. Choose one with the topic-pattern. | No       |
| topic-pattern                 | null          | Use regular to get the matching topic.                       | No       |
| service-url                   | null          | Set the Pulsar broker service address.                               | Yes      |
| admin-url                     | null          | Set the Pulsar administration service address.                                 | Yes      |
| scan.startup.mode             | latest        | COnfigure the Source's startup mode,. Available options are `earliest`, `latest`, `external-subscription`, and `specific-offsets`. | No       |
| scan.startup.specific-offsets | null          | This parameter is required when the `specific-offsets` parameter is specified. | No       |
| scan.startup.sub-name         | null          | This parameter is required for the External subscription mode . | No       |
| discovery topic interval      | null          | Set the time interval for partition discovery, in unit of milliseconds.         | No       |
| sink.message-router           | key-hash      | Set the routing method for writing messages to the Pulsar partition. Available options are `key-hash`, `round-robin`, and `custom MessageRouter`. | No       |
| sink.semantic                 | at-least-once | The Sink writes the assurance level of the message. Available options are `at-least-once`, `exactly-once`, and `none`. | No       |
| properties                    | empty         | Set Pulsar's optional configurations, in a format of `properties.key='value'`. For details, see [Configuration parameters](#configuration-parameters). | No       |
| key.format                    | null          | Set the key-based serialization format for Pulsar messages. Available options are `No format`, `optional raw`, `Avro`, `JSON`, etc. | No       |
| key.fields                    | null          | The SQL definition field to be used when serializing Key, multiple by half comma `,` concatenated. | No       |
| key.fields-prefix             | null          | Define a custom prefix for all fields in the key format to avoid name conflicts with fields in the value format. By default, the prefix is empty. If a custom prefix is defined, the Table schema and `key.fields` are used. | No       |
| format or value.format        | null          | Set the name with a prefix. When constructing data types in the key format, the prefix is removed and non-prefixed names are used within the key format. Pulsar message value serialization format, support JSON, Avro, etc. For more information, see the Flink format. | Yes      |
| value.fields-include          | ALL           | The Pulsar message value contains the field policy, optionally ALL, and EXCEPT_KEY. | No       |

#### Metadata configurations

The METADATA flag is used to read and write metadata in Pulsar messages. The support list is as follows.

> **Note**  
> The R/W column defines whether a metadata field is readable (R) and/or writable (W). Read-only columns must be declared VIRTUAL to exclude them during an INSERT INTO operation.

| Key         | Data Type                                  | Description                            | R/W  |
| ----------- | ------------------------------------------ | -------------------------------------- | ---- |
| topic       | STRING NOT NULL                            | Topic name of the Pulsar message.      | R    |
| messageId   | BYTES NOT NULL                             | Message ID of the Pulsar message.      | R    |
| sequenceId  | BIGINT NOT NULL                            | sequence ID of the Pulsar message.     | R    |
| publishTime | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Publishing time of the Pulsar message. | R    |
| eventTime   | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Generation time of the Pulsar message. | R/W  |
| properties  | MAP<STRING, STRING> NOT NULL               | Extensions information of the Pulsar message. | R/W  |

### Catalog

Flink always searches for tables, views and UDFs in the current catalog and database. To use the Pulsar Catalog and treat the topic in Pulsar as a table in Flink, you should use the `pulsarcatalog` that has been defined in `./conf/sql-client-defaults.yaml` in `pulsarcatalog`.

```java
tableEnv.useCatalog("pulsarcatalog")
tableEnv.useDatabase("public/default")
tableEnv.scan("topic0")
```

```SQL
Flink SQL> USE CATALOG pulsarcatalog;
Flink SQL> USE `public/default`;
Flink SQL> select * from topic0;
```

The following configuration is optional in the environment file, or it can be overridden in the SQL client session using the `SET` command.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr >
<tr>
 <td>`default-database`</td>
 <td>Default database name </td>
 <td>public/default</td>
 <td>When using the Pulsar catalog, the topic in Pulsar is treated as a table in Flink. Therefore, `database` is another name for `tenant/namespace`. The database is the base path for table lookups or creation. </td>
 </tr>
 <tr>
 <td>`table-default-partitions`</td>
 <td>Default topic partition</td>
 <td>5</td>
 <td>When using the Pulsar catalog, the topic in Pulsar is treated as a table in Flink. The size of the partition is set when creating the topic. </td>
 </tr>
</table>

For more details, see [DDL configurations](#ddl-configurations).

> **Note**  
> In Catalog, you cannot delete `tenant/namespace` or `topic`.

# Advanced features

This section describes advanced features supported by Pulsar Flink connector.

## Pulsar primitive types

Pulsar provides some basic native types. To use these native types, you can support them in the following ways.

### Stream API environment

PulsarPrimitiveSchema is an implementation of the `PulsarDeserializationSchema` and `PulsarSerializationSchema` interfaces.

You can create the required instance in a similar way `new PulsarSerializationSchema(String.class)`.

### Table environment

We have created a new Flink format component called `atomic` that you can use in SQL format. In Source, it translates the Pulsar native type into only one column of RowData. In Sink, it translates the first column of RowData into the Pulsar native type and writes it to Pulsar.

## Upsert Pulsar

There is an increasing demand for Upsert mode message queues for three main reasons.

- Interpret the Pulsar topic as a changelog stream, which interprets records with keys as Upsert events.
- As part of the real-time pipeline, multiple streams are connected for enrichment and the results are stored in the Pulsar topic for further computation. However, the results may contain updated events.
- As part of the real-time pipeline, the data stream is aggregated and the results are stored in Pulsar Topic for further computation. However, the results may contain updated events.
  
Based on these requirements, we support Upsert Pulsar. With this feature, users can read data from and write data to Pulsar topics in an Upsert fashion.

In the SQL DDL definition, you can set the connector to `upsert-pulsar` to use the Upsert Pulsar connector.

In terms of configuration, the primary key of the Table must be specified, and `key.fields` cannot be used.

As a source, the Upsert Pulsar connector produces changelog streams, where each data record represents an update or deletion event. More precisely, the value in a data record is interpreted as a UPDATE of the last value of the same key, if this key exists (If the corresponding key does not exist, the UPDATE is considered as an INSERT.). Using the table analogy, data records in the changelog stream are interpreted as UPSERT, also known as INSERT/UPDATE, because any existing row with the same key is overwritten. Also, a message with a null value is treated as a DELETE message.

As a sink, the Upsert Pulsar connector can consume changelog streams. It writes INSERT/UPDATE_AFTER data as normal Pulsar messages and writes DELETE data as Pulsar messages with null value (It indicates that key of the message is deleted). Flink partitions the data based on the value of the primary key so that the messages on the primary key are ordered. And, UPDATE/DELETE messages with the same primary key fall in the same partition.

## Key-Shared subscription mode

In some scenarios, users need messages to be strictly guaranteed message order to ensure correct business processing. Usually, in the case of strictly order-preserving messages, only one consumer can consume messages at the same time to guarantee the order. This results in a significant reduction in message throughput. Pulsar designs the Key-Shared subscription mode for such scenarios by adding keys to messages and routing messages with the same Key Hash to the same messenger, which ensures message order and improves throughput.

Pulsar Flink connector supports this feature the as well. This feature can be enabled by configuring the `enable-key-hash-range=true` parameter. When enabled, the range of Key Hash processed by each consumer is divided based on the parallelism of the task.

## Fault tolerance

Pulsar Flink connector 2.7.0 provides different semantics for source and sink.

### Source

For Pulsar source, Pulsar Flink connector 2.7.0 provides `exactly-once` semantic.

### Sink

Pulsar Flink connector 2.4.12 only supports `at-least-once` semantic for sink. Based on transactions supported in Pulsar 2.7.0 and the Flink [`TwoPhaseCommitSinkFunction` API](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/sink/TwoPhaseCommitSinkFunction.html), Pulsar Flink connector 2.7.0 supports both `exactly-once` and `at-least-once` semantics for sink. For more information, see [here](https://flink.apache.org/2021/01/07/pulsar-flink-connector-270.html).

Before setting `exactly_once` semantic for a sink, you need to make the following configuration changes.

1. In Pulsar, transaction related functions are **disabled by default**. In this case, you need to set `transactionCoordinatorEnabled = true` in the configuration file (`conf/standalone.conf` or `conf/broker.conf`) .

2. When creating a sink, set `PulsarSinkSemantic.EXACTLY_ONCE`. The default value of  `PulsarSinkSemantic` is `AT_LEAST_ONCE`.

    Example

    ```
    SinkFunction<Integer> sink = new FlinkPulsarSink<>(
          adminUrl,
          Optional.of(topic),
          clientConfigurationData,
          new Properties(),
          new PulsarSerializationSchemaWrapper.Builder<>
                  ((SerializationSchema<Integer>) element -> Schema.INT32.encode(element))
                  .useAtomicMode(DataTypes.INT())
                  .build(),
          PulsarSinkSemantic.EXACTLY_ONCE
    );
    ```

    Additionally, you can set transaction related configurations as below.

    Parameter|Description|Default value
    ---|---|---
    `PulsarOptions.TRANSACTION_TIMEOUT`|Timeout for transactions in Pulsar. If the time exceeds, the transaction operation fails.|360000ms
    `PulsarOptions.MAX_BLOCK_TIME_MS`|Maximum time to wait for a transaction to commit or abort. If the time exceeds, the operator throws an exception.|100000ms

    Alternatively, you can override these configurations in the `Properties` object and pass it into the `Sink` constructor.

## Configuration parameters

This parameter corresponds to the `FlinkPulsarSource` in StreamAPI, the Properties object in the FlinkPulsarSink construction parameter, and the configuration properties parameter in Table mode.

| Parameter | Default value | Description | Effective range |
| --------- | -------- | ---------------------- | ------------ |
| topic | null | Pulsar topic | source |
| topics | null | Multiple Pulsar topics connected by half-width commas | source |
| topicspattern | null | Multiple Pulsar topics with more Java regular matching | source |
| partition.discovery.interval-millis | -1 | Automatically discover added or removed topics, in unit of milliseconds. If the value is set to -1, it indicates that means not open. | source |
| clientcachesize | 100 | Set the number of cached Pulsar clients. | source, sink |
| auth-params | null | Set the authentication parameters for Pulsar clients. | source, sink |
| auth-plugin-classname | null | Set the authentication class name for Pulsar clients.  | source, sink |
| flushoncheckpoint | true | Write a message to Pulsar topics. | sink |
| failonwrite | false | When sink error occurs, continue to confirm the message. | sink |
| polltimeoutms | 120000 | Set the timeout for waiting to get the next message, in unit of milliseconds. | source |
| failondataloss | true | When data is lost, the operation fails. | source |
| commitmaxretries | 3 | Set the maximum number of retries when an offset is set for Pulsar messages. | source |
| scan.startup.mode | null | Set the earliest, latest, and the position where subscribers consume news,. It is a required parameter. | source |
| enable-key-hash-range | false | Enable the Key-Shared subscription mode. | source |
| pulsar.reader.* | | For details about Pulsar reader configurations, see [Pulsar reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader). | source |
| pulsar.reader.subscriptionRolePrefix | flink-pulsar- | When no subscriber is specified, the prefix of the subscriber name is automatically created. | source |
| pulsar.reader.receiverQueueSize | 1000 | Set the receive queue size. | source |
| pulsar.producer.* | | For details about Pulsar producer configurations, see [Pulsar producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer). | Sink |
| pulsar.producer.sendTimeoutMs | 30000 | Set the timeout for sending a message, in unit of milliseconds. | Sink |
| pulsar.producer.blockIfQueueFull | false | The Pulsar producer writes messages. When the queue is full, the method is blocked instead of an exception is thrown. | Sink |

`pulsar.reader.*` and `pulsar.producer.*` specify more detailed configuration of the Pulsar behavior. The asterisk sign (*) is replaced by the configuration name in Pulsar. For details, see [Pulsar reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader) and [Pulsar producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer).

In the DDL statement, the sample which is similar to the following is used.

```
'properties.pulsar.reader.subscriptionRolePrefix' = 'pulsar-flink-',
'properties.pulsar.producer.sendTimeoutMs' = '30000',
```

## Authentication configuration

For Pulsar instances configured with authentication, the Pulsar Flink connector can be configured in a similar as the regular Pulsar client.

For `FlinkPulsarSource` and `FlinkPulsarSink`, you can use one of the following ways to set up authentication.

- Set the `Properties` parameter.

  ```java
  props.setProperty(PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY, "org.apache.pulsar.client.impl.auth.AuthenticationToken");
  props.setProperty(PulsarOptions.AUTH_PARAMS_KEY, "token:abcdefghijklmn");
  ```

- Set the `ClientConfigurationData` parameter, which has a higher priority than the `Properties` parameter.

  ```java
  ClientConfigurationData conf = new ClientConfigurationData();
  conf.setServiceUrl(serviceUrl);
  conf.setAuthPluginClassName(className);
  conf.setAuthParams(params);
  ```

For details about authentication configuration, see [Pulsar Security](https://pulsar.apache.org/docs/en/security-overview/).

## ProtoBuf supports [experimental features].

This feature is based on [Flink: New Format of protobuf](https://github.com/apache/flink/pull/14376) and is currently pending merge.
Example of using protobuf in sql: 
```
create table pulsar (
                        a INT,
                        b BIGINT,
                        c BOOLEAN,
                        d FLOAT,
                        e DOUBLE,
                        f VARCHAR(32),
                        g BYTES,
                        h VARCHAR(32),
                        f_abc_7d INT,
                        `eventTime` TIMESTAMP(3) METADATA,
                        compute as a + 1,
                        watermark for eventTime as eventTime
                        ) with (
                        'connector' = 'pulsar',
                        'topic' = 'test-protobuf',
                        'service-url' = 'pulsar://localhost:6650',
                        'admin-url' = 'http://localhost:8080',
                        'scan.startup.mode' = 'earliest',
                        'format' = 'protobuf',
                        'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.SimpleTest'
                        )

INSERT INTO pulsar VALUES (1,2,false,0.1,0.01,'haha', ENCODE('1', 'utf-8'), 'IMAGES',1, TIMESTAMP '2020-03-08 13:12:11.123');
```
Requirement: `SimpleTest` class must implement `GeneratedMessageV3`.
Since the Flink Format: ProtoBuf component has not been merged, it is temporarily placed in this repository as a source code for packaging and dependencies.
