# Pulsar Flink Connector
The Pulsar Flink connector implements elastic data processing using [Apache Pulsar](https://pulsar.apache.org) and [Apache Flink](https://flink.apache.org).

Chinese document: [README_CN](doc/README_CN.md)

# Prerequisites

-  Java 8 or higher
-  Flink 1.9.0 or higher
-  Pulsar 2.4.0 or higher

# Basic

## Client

Multiple versions of Flink are now supported.

-  Flink 1.9 - 1.10 maintained in branch `flink-1.9`

-  Flink 1.11 is maintained in branch `flink-1.11`

-  Flink 1.12 is the current master supported version, in the `master` branch

> Since Flink's API has changed a lot, we mainly work on new features in master, and the rest of the branches focus on bug fixes.



For projects using SBT, Maven, Gradle, the following parameters can be set to your project.

-  The `FLINK_VERSION` parameter is now available as `1.9`, `1.11`, and `1.12`.

-  The `SCALA_BINARY_VERSION` parameter is related to the scala version used by flink, `2.11`, `2.12` are available.
-  The `PULSAR_FLINK_VERSION` is the version of this connector. Usually only the three-digit version `2.7.0` is 
   available, but the four-digit version `2.7.0.1` is used when providing bug fixes.

```
     groupId = io.streamnative.connectors
     artifactId = pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{FLINK_VERSION}}
     version = {{PULSAR_FLINK_VERSION}}
```
The jar package is located in the [Bintray Maven repository of StreamNative](https://dl.bintray.com/streamnative/maven) 。


Maven projects can be added to the repository configuration to your `pom.xml` with the following content.

```xml
 <repositories>
     <repository>
       <id>central</id>
       <layout>default</layout>
       <url>https://repo1.maven.org/maven2</url>
     </repository>.
     <repository>
       <id>bintray-streamnative-maven</id
       <name>bintray</name>
       <url>https://dl.bintray.com/streamnative/maven</url>
     </repository>.
 </repositories>.
```
Gradle projects can add the repository configuration in `build.gradle`, as follows.

```groovy
repositories {
         maven {
             url 'https://dl.bintray.com/streamnative/maven'
         }
}
```

For maven projects, to build an application JAR that contains all the dependencies required for the library and pulsar flink connector, you can use the following shade plugin definition template.

```xml
<plugin>
 <!-- Shade all the dependencies to avoid conflicts -->
 <groupId>org.apache.maven.plugins</groupId>
 <artifactId>maven-shade-plugin</artifactId>
 <version>${maven-shade-plugin.version}</version
 <executions
     <execution
       <phase>package</phase>
       <goals>
         <goal>shade</goal
       </goals>.
       <configuration>
         <createDependencyReducedPom>true</createDependencyReducedPom>
         <promoteTransitiveDependencies>true</promoteTransitiveDependencies
         <minimizeJar>false</minimizeJar>

         <artifactSet>
           <includes>
             <include>io.streamnative.connectors:*</include>
             <!-- more libs to include here -->
           </includes>
         </artifactSet>.
         <filters>
           <filter>
             <artifact>*:*</artifact>
             <excludes>
               <exclude>META-INF/*.SF</exclude>
               <exclude>META-INF/*.DSA</exclude>
               <exclude>META-INF/*.RSA</exclude>
             </excludes>
           </filter>.
         </filters
         <transformers>
           <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
           <transformer implementation="org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer" />
         </transformers>.
       </configuration>.
     </execution>.
 </executions>.
</plugin
```

For gradle projects, to build an application JAR containing all the dependencies needed for the library and pulsar flink connector, you can define a template using the following [shade](https://imperceptiblethoughts.com/shadow/) plugin.

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





## Deployment

### Client library
As with any Flink application, `. /bin/flink run` is used to compile and start your application.

If you have already built a jar with dependencies using the shade plugin above, you can add your jar to `flink run` using `--classpath`.

> #### Note
> The path must be in a protocol format (e.g., `file://`) and the path must be accessible on all nodes.

Example:

```
$ . /bin/flink run
 -c com.example.entry.point.ClassName file://path/to/jars/your_fat_jar.jar
 ...
```

### Scala REPL
Try it in the interactive Scala shell `bin/start-scala-shell.sh`, you can add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{ PULSAR_FLINK_VERSION}}.jar`.

Example:

```
$ . /bin/start-scala-shell.sh remote <hostname> <portnumber>
 --addclasspath pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
For more information on submitting applications using the CLI, please refer to [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/cli.html) .


### SQL Client
To use [SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sqlClient.html) and write 
SQL queries to manipulate the data in Pulsar, you can use the  `-addclasspath` parameter to directly add `pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`.

Example:
```
$ ./bin/sql-client.sh embedded --jar pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
> If you put the jar of our connector under `$FLINK_HOME/lib`, please do not use `-jar` again to specify the package of the connector.

By default, to use the Pulsar directory in SQL Client and register it automatically at startup, SQL Client reads its configuration from the environment file `. /conf/sql-client-defaults.yaml` to read its configuration. You need to add the Pulsar catalog to the `catalogs` section of this YAML file.

```yaml
catalogs:
-  me: pulsarcatalog
     type: pulsar
     default-database: tn/ns
     service-url: "pulsar://localhost:6650"
     admin-url: "http://localhost:8080"
     format: json
```





# Stream environment

## Source

Flink's Pulsar consumer is called `FlinkPulsarSource<T>`. It provides access to one or more Pulsar topics.

Its constructor method has the following parameters.

1. the service address `serviceUrl` and the administrative address `adminUrl` used to connect to the Pulsar instance.
2. When using `FlinkPulsarSource`, you need to set `PulsarDeserializationSchema<T>`.
3. the Properties parameter, which is used to configure the behavior of the Pulsar Consumer.
   Properties necessary parameters, as follows:

    -  e and only one of these parameters `topic`, `topics` or `topicsPattern` must have a value present. This is used to configure the Topic information to be consumed. (**`topics` is multiple topics separated by a comma `,`, `topicsPattern` is a java regular expression that matches a number of topics**)
4. Set consumption mode can be set by setStartFromLatest, setStartFromEarliest, setStartFromSpecificOffsets, setStartFromSubscription, etc. of FlinkPulsarSource. When using setStartFromSubscription subscriber mode, the checkpoint function must be enabled.


Example:

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic");
props.setProperty("partition.discovery.interval-millis", "5000");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, new PulsarDeserializationSchemaWrapper(new SimpleStringSchema(),DataTypes.STRING()), props);

// or setStartFromLatest、setStartFromSpecificOffsets、setStartFromSubscription
source.setStartFromEarliest(); 

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```



## Sink

The Pulsar producer uses the `FlinkPulsarSink` instance. It allows to write record streams to one or more Pulsar Topics.

Example:

```java
PulsarSerializationSchema<Person> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(Person.class))
.usePojoMode(Person. class, RecordSchemaType.JSON)
.setTopicExtractor(person -> null)
.build();
FlinkPulsarSink<Person> sink = new FlinkPulsarSink(
 serviceUrl,
 adminUrl,
 Optional.of(topic),       // mandatory target topic or use `Optional.empty()` if sink to different topics for each record
 props,
 pulsarSerialization,
 PulsarSinkSemantic.AT_LEAST_ONCE);

stream.addSink(sink);
```



## PulsarDeserializationSchema

PulsarDeserializationSchema is a connector-defined Flink DeserializationSchema wrapper that allows flexible manipulation of Pulsar Message.

PulsarDeserializationSchemaWrapper is a simple implementation of PulsarDeserializationSchema with two parameters: Flink DeserializationSchema and information about the decoded message type.

```
PulsarDeserializationSchemaWrapper(new SimpleStringSchema(),DataTypes.STRING())
```



> DataTypes type from flink's `table-common` module.



## PulsarSerializationSchema

PulsarSerializationSchema is a wrapper for Flink SerializationSchema that provides more functionality. In most cases, you don't need to implement PulsarSerializationSchema yourself, we provide PulsarSerializationSchemaWrapper to wrap a Flink SerializationSchema as  PulsarSerializationSchema.

PulsarSerializationSchema uses the builder pattern and you can call setKeyExtractor or setTopicExtractor to satisfy the need to extract the key and customize the target Topic from each message.

In particular, since Pulsar maintains its own Schema information internally, our messages must be able to export a SchemaInfo when they are written to Pulsar. The useSpecialMode, useAtomicMode, usePojoMode, and useRowMode methods can help you quickly build the Schema information you need for Pulsar. You must choose only one of these four modes.

-  SpecialMode: Specify directly the `Schema<? >` mode, make sure this Schema is compatible with your setting of 
   Flink SerializationSchema.

-  AtomicMode: For some atomic types, pass the type of AtomicDataType, such as `DataTypes.INT()`, which will 
   correspond to `Schema<Integer>` in Pulsar.
-  PojoMode: You need to pass a custom Class object and one of Json or Arvo to specify the way to build a composite 
   type Schema. For example `usePojoMode(Person.class, RecordSchemaType.JSON)`.

-  RowMode: In general, you will not use this mode, it is used for our internal Table&SQL API implementation.



## Fault tolerance

With Flink's checkpointing enabled, `FlinkPulsarSink` can provide at-least-once, exactly-once delivery guarantees.

In addition to enabling checkpoints for Flink, you should also configure `setLogFailuresOnly(boolean)` and `setFlushOnCheckpoint(boolean)`.

*`setFlushOnCheckpoint(boolean)`*: by default, it is set to `true`. When this is enabled, writing to pulsar records will be executed at this checkpoint snapshotState. This ensures that all records before checkpoint are written to pulsar. Note that flink's at-least-once setting must also be enabled.



# Table environment

The Pulsar-flink connector fully supports the Table feature, covering the following list.

-  SQL, DDL
-  Catalog

## SQL, DDL

SQL Example

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

SQL is complete with support for physical fields, calculated columns, watermark, METADATA and other features.



### DDL configuration support list

| Parameters                    | Default Value | Description                                                  | Required |
| ----------------------------- | ------------- | ------------------------------------------------------------ | -------- |
| connector                     | null          | The connectors used are available as pulsar and upsert-pulsar. | Yes      |
| topic                         | null          | Input or output topic, use half comma  when multiple, concatenate. Choose one with topic-pattern | No       |
| topic-pattern                 | null          | Use regular to get the matching Topic.                       | No       |
| service-url                   | null          | Pulsar broker service address                                | Yes      |
| admin-url                     | null          | Pulsar admin service address                                 | Yes      |
| scan.startup.mode             | latest        | Source's startup mode, options earliest, latest, external-subscription, specific-offsets | No       |
| scan.startup.specific-offsets | null          | When using specific-offsets, the message offset must be specified | No       |
| scan.startup.sub-name         | null          | Must be set when using the subscription mode (external-subscription). | No       |
| discovery topic interval      | null          | Time interval for partition discovery, milliseconds          | No       |
| sink.message-router           | key-hash      | Routing method for writing messages to the Pulsar partition, with options for key-hash, round-robin, and custom MessageRouter implementation class reference paths | No       |
| sink.semantic                 | at-least-once | Sink writes the assurance level of the message. Options at-least-once, exactly-once, none | No       |
| properties                    | empty         | Pulsar's optional configuration set,  format `properties.key='value'`, refer to #Configuration Parameters | No       |
| key.format                    | null          | Pulsar Message's Key serialization No format, optional raw, avro, json, etc. | No       |
| key.fields                    | null          | The SQL definition field to be used when serializing Key, multiple by half comma `,` concatenated. | No       |
| key.fields-prefix             | null          | Define a custom prefix for all fields in the key format to avoid name conflicts  with fields in the value format. By  default, the prefix is empty. If a custom prefix is defined, the Table schema and 'key.fields' will both use the | No       |
| format or value.format        | null          | name with the prefix. When constructing data types in key format, the prefix will be removed and non- prefixed names will be used within the key format.Pulsar message value serialization format, support json, avro, etc., for<br/> more reference Flink format. | Yes      |
| value.fields-include          | ALL           | Pulsar message value contains field policy, optionally ALL, EXCEPT_KEY | No       |



### Pulsar Message metadata manipulation

The METADATA flag is used to read and write metadata in Pulsar Message. The support list is as follows.

**The R/W column defines whether a metadata field is readable (R) and/or writable (W). Read-only columns must be declared VIRTUAL to exclude them during an INSERT INTO operation.**

| Key         | Data Type                                  | Description                            | R/W  |
| ----------- | ------------------------------------------ | -------------------------------------- | ---- |
| topic       | STRING NOT NULL                            | Topic name of the Pulsar Message.      | R    |
| messageId   | BYTES NOT NULL                             | MessageId of the Pulsar Message.       | R    |
| sequenceId  | BIGINT NOT NULL                            | Pulsar Message sequence Id             | R    |
| publishTime | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Pulsar message published time          | R    |
| eventTime   | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Message Generation Time                | R/W  |
| properties  | MAP<STRING, STRING> NOT NULL               | Pulsar Message Extensions Information. | R/W  |





## Catalog

Flink always searches for tables, views and UDFs in the current catalog and database. To use the Pulsar Catalog and treat the topic in Pulsar as a table in Flink, you should use the `pulsarcatalog` that has been defined in `. /conf/sql-client-defaults.yaml` defined in `pulsarcatalog`. .

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

The following configuration is optional in the environment file, or can be overridden in the SQL client session using the `SET` command.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr >
<tr>
 <td>`default-database`</td>
 <td>Default database name. </td>
 <td>public/default</td>
 <td>When using the Pulsar catalog, the topic in Pulsar is treated as a table in Flink, so `database` is another name for `tenant/namespace`. The database is the base path for table lookups or creation. </td>
 </tr>
 <tr>
 <td>`table-default-partitions`</td>
 <td>Topic default partition</td>
 <td>5</td>
 <td>When using the Pulsar catalog, the topic in Pulsar is treated as a table in Flink. The size of the partition set when creating the Topic. </td>
 </tr>
</table>


Refer to the configuration items in the DDL for more details on the parameters

Note: Due to the dangerous nature of the delete operation, the delete `tenant/namespace` and `topic` operations in Catalog are not supported at the moment.




# Advanced Features



## Pulsar primitive types

Pulsar itself provides some basic native types, and if you need to use native types, you can support them in the following way



### Stream API environment

PulsarPrimitiveSchema is an implementation of the `PulsarDeserializationSchema`, `PulsarSerializationSchema` interfaces.

You can create the required instance in a similar way `new PulsarSerializationSchema(String.class)`.



### Table Environment

We have created a new Flink format component called `atomic` that you can use in SQL format. In Source, it translates the Pulsar native type into only one column worth of RowData. in Sink, it translates the first column of RowData into the Pulsar native type and writes it to Pulsar.



## Upsert Pulsar

There is a high demand for Upsert mode message queues from Flink community users for three main reasons.

-  Interpret Pulsar Topic as a changelog stream, which interprets records with keys as upsert events.
-   As part of the real-time pipeline, multiple streams are connected for enrichment and the results are stored in Pulsar Topic for further computation later. However, the results may contain update events.
-   As part of the real-time pipeline, the data stream is aggregated and the results are stored in Pulsar Topic for further computation later. However, the results may contain update events.
    Based on these requirements, we have also implemented support for Upsert Pulsar. Using this feature, users can read data from and write data to Pulsar topics in an upsert fashion.



In the SQL DDL definition, you set the connector to upsert-pulsar to use the Upsert Pulsar connector.

In terms of configuration, the primary key of the Table must be specified, and `key.fields` cannot be used.

As source, the upsert-pulsar connector produces changelog streams, where each data record represents an update or deletion event. More precisely, the value in a data record is interpreted as a UPDATE of the last value of the same key, if this key exists (if the corresponding key does not exist, the update is considered as an INSERT). Using the table analogy, data records in the changelog stream are interpreted as UPSERT, also known as INSERT/UPDATE, because any existing row with the same key is overwritten. Also, a message with a null value will be treated as a DELETE message.

As a sink, the upsert-pulsar connector can consume changelog streams. It writes INSERT/UPDATE_AFTER data as normal Pulsar messages and writes DELETE data as Pulsar messages with null value (indicating that the message corresponding to the key is deleted). Flink will partition the data based on the value of the primary key column so that the messages on the primary key are ordered, so that update/delete messages on the same primary key will fall in the same partition.



## Pulsar Key-Shared

High Performance Ordered Message Queues In some scenarios, users need messages to be strictly guaranteed message order in order to ensure correct business processing. Usually, in the case of strictly order-preserving messages, only one consumer can consume messages at the same time to guarantee the order. This results in a significant reduction in message throughput. pulsar designs the Key-Shared subscription pattern for such scenarios by adding Keys to messages and routing messages with the same Key Hash to the same messenger, which ensures message order and improves throughput.

We have added support for this feature in the Pulsar Flink connector as well. This feature can be enabled by configuring the parameter `enable-key-hash-range=true`. When enabled, the range of Key Hash processed by each consumer will be divided according to the parallelism of the task.

## Configuration Parameters

This parameter corresponds to the FlinkPulsarSource in StreamAPI, the Properties object in the FlinkPulsarSink construction parameter, and the configuration properties parameter in Table mode.

| Parameters | Default Value | Description | Effective Range |
| --------- | -------- | ---------------------- | ------------ |
| topic | null | pulsar topic | source |
| topics | null | Multiple pulsar topics connected by half-width commas | source |
| topicspattern | null | Multiple pulsar topics with more java regular matching | source |
| partition.discovery.interval-millis | -1 | Automatically discover increase and decrease topics, milliseconds. -1 means not open. | source |
| clientcachesize | 100 | Number of cached pulsar clients | source, sink |
| auth-params | null | pulsar clients auth params | source, sink |
| auth-plugin-classname | null | pulsar clients auth class name | source, sink |
| flushoncheckpoint | true | Write a message to pulsar | sink |
| failonwrite | false | When sink error occurs, continue to confirm the message | sink |
| polltimeoutms | 120000 | The timeout period for waiting to get the next message, milliseconds | source |
| failondataloss | true | Does it fail when data is lost | source |
| commitmaxretries | 3 | Maximum number of retries when offset to pulsar message | source |
| scan.startup.mode | null | earliest, latest, the position where subscribers consume news, required | source |
| enable-key-hash-range | false | enable pulsar Key-Shared mode | source |
| pulsar.reader.* | | For detailed configuration of pulsar consumer, please refer to [pulsar reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader) | source |
| pulsar.reader.subscriptionRolePrefix | flink-pulsar- | When no subscriber is specified, the prefix of the subscriber name is automatically created | source |
| pulsar.reader.receiverQueueSize | 1000 | Receive queue size | source |
| pulsar.producer.* | | For detailed configuration of pulsar consumer, please refer to [pulsar producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer) | Sink |
| pulsar.producer.sendTimeoutMs | 30000 | Timeout time when sending a message, milliseconds | Sink |
| pulsar.producer.blockIfQueueFull | false | Producer writes messages. When the queue is full, block the method instead of throwing an exception | Sink |

`pulsar.reader.*` and `pulsar.producer.*` specify more detailed configuration of pulsar's behavior, * is replaced by the configuration name in pulsar, refer to the link in the table for the contents.



In the DDL statement, the sample used is as follows:

```
'properties.pulsar.reader.subscriptionRolePrefix' = 'pulsar-flink-',
'properties.pulsar.producer.sendTimeoutMs' = '30000',
```



## Authentication Configuration

For Pulsar instances with authentication configured, the Pulsar Flink connector can be set up in a similar manner to the regular Pulsar client.

For FlinkPulsarSource, FlinkPulsarSink, you have two ways to set up authentication

-  Connstructed parameters `Properties`

 ```java
 props.setProperty(PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY, "org.apache.pulsar.client.impl.auth.AuthenticationToken");
 props.setProperty(PulsarOptions.AUTH_PARAMS_KEY, "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.2AgtxHe8- 2QBV529B5DrRtpuqP6RJjrk21Mhnomfivo");
 ```

-  Constructs the parameter `ClientConfigurationData`, which has a higher priority than `Properties`.

 ```java
 ClientConfigurationData conf = new ClientConfigurationData();
 conf.setServiceUrl(serviceUrl);
 conf.setAuthPluginClassName(className);
 conf.setAuthParams(params);
 ```

For detailed configuration of authentication, please refer to [Pulsar Security](https://pulsar.apache.org/docs/en/security-overview/).





# Build Pulsar Flink Connector
To build the Pulsar Flink connector expecting to read data from Pulsar or write the results to Pulsar, follow these steps
1. Check out the source code
     ```bash
     $ git clone https://github.com/streamnative/pulsar-flink.git
     $ cd pulsar-flink
     ```

2. install Docker

   The Pulsar-flink connector is using [Testcontainers](https://www.testcontainers.org/) for integration testing. In order to run the integration tests, make sure that [Docker](https://docs.docker.com/docker-for-mac/install/) is installed.

3. set the Java version

   Modify `java.version` and `java.binary.version` in `pom.xml`.
   > #### Note
   > Java version should be the same as the Java version of flink you are using.

4. Build project
     ```bash
     $ mvn clean install -DskipTests
     ```

5. Run the test
     ```bash
     $ mvn clean install
     ```
After the installation is complete, a jar containing the dependencies will be generated in both the local maven repo and `target` directories.

> Note: If you use intellij IDEA to debug this project, you may encounter the package `org.apache.pulsar.shade.org.bookkeeper.ledger` error. Workaround:First run ` mvn clean install -DskipTests` to install the jar to the local repository, then ignore the maven module `managed-ledger-shaded` on the project. The error disappears after refreshing the project.

