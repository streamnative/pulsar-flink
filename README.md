# pulsar-flink
Pulsar Flink connector is an elastic data processing with [Apache Pulsar](https://pulsar.apache.org) and [Apache Flink](https://flink.apache.org).

## Prerequisites

- Java 8 or later
- Flink 1.9.0 or later
- Pulsar 2.4.0 or later

## Preparations

### Link

#### Client library  
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

```
    groupId = io.streamnative.connectors
    artifactId = pulsar-flink-connector_{{SCALA_BINARY_VERSION}}
    version = {{PULSAR_FLINK_VERSION}}
```
Currently, the artifact is available in [Bintray Maven repository of StreamNative](https://dl.bintray.com/streamnative/maven).
For Maven project, you can add the repository to your `pom.xml` as follows:
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
To build an application JAR that contains all dependencies required for libraries and pulsar flink connector,
you can use the following shade plugin definition template:

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

### Deploy

#### Client library  
As with any Flink applications, `./bin/flink run` is used to compile and launch your application.     
If you have already built a fat jar using the shade maven plugin above, your jar can be added to `flink run` using `--classpath`.

> #### Note
> The format of a path must be a protocol (for example, `file://`) and the path should be accessible on all nodes.

Example

```
$ ./bin/flink run
  -c com.example.entry.point.ClassName file://path/to/jars/your_fat_jar.jar
  ...
```

#### Scala REPL  
For experimenting on the interactive Scala shell `bin/start-scala-shell.sh`, you can use `--addclasspath` to add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar` directly.

Example

```
$ ./bin/start-scala-shell.sh remote <hostname> <portnumber>
  --addclasspath pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
For more information about **submitting applications with CLI**, see [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/cli.html).


#### SQL Client
For playing with [SQL Client Beta](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sqlClient.html) and writing queries in SQL to manipulate data in Pulsar, you can use `--jar` to add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar` directly.

Example
```
$ ./bin/sql-client.sh embedded --jar pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
By default, to use Pulsar catalog in SQL Client and get it registered automatically at startup, the SQL Client reads its configuration from the environment file `./conf/sql-client-defaults.yaml`. You need to add Pulsar catalog to `catalogs` section in this YAML file:

```yaml
catalogs:
- name: pulsarcatalog
    type: pulsar
    default-database: tn/ns
    service-url: "pulsar://localhost:6650"
    admin-url: "http://localhost:8080"
```

## Pulsar Source

Flink's Pulsar consumer is called `FlinkPulsarSource<T>`
or just `FlinkPulsarRowSource` with data schema auto-inferring). It provides access to one or more Pulsar topics.

The constructor accepts the following arguments:

1. The service url and admin url for the Pulsar instance to connect to.
2. A DeserializationSchema for deserializing the data from Pulsar when using `FlinkPulsarSource`
3. Properties for the Pulsar Source.
  The following properties are required:
  - One of "topic", "topics" or "topicsPattern" to denote topic(s) to consume. (**topics is a comma-separated list of topics, and topicsPattern is a Java regex string used to pattern matching topic names **)

Example:

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic")
FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, new SimpleStringSchema(), props);

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```

### The `DeserializationSchema`

When `FlinkPulsarSource<T>` is used, it needs to know how to turn the binary data in Pulsar into Java/Scala objects. The
`DeserializationSchema` allows users to specify such a schema. The `T deserialize(byte[] message)`
method gets called for each Pulsar message, passing the value from Pulsar.


It is usually helpful to start from the `AbstractDeserializationSchema`, which takes care of describing the
produced Java/Scala type to Flink's type system. Users that implement a vanilla `DeserializationSchema` need
to implement the `getProducedType(...)` method themselves.

For convenience, we provide the following implementations of the `DeserializationSchema` interface:

1. `JsonDeser`: if the topic is of JSONSchema in Pulsar, you could use `JsonDeser.of(POJO_CLASS_NAME.class)` for `DeserializationSchema`.

2. `AvroDeser`: if the topic is of AVROSchema in Pulsar, you could use `AvroDeser.of(POJO_CLASS_NAME.class)` for `DeserializationSchema`.


### Schema for FlinkPulsarRowSource

  - For topics without schema or with primitive schema in Pulsar, messages payload
  is loaded to a `value` column with the corresponding type with Pulsar schema.

  - For topics with Avro or JSON schema, their field names and field types are kept in the result rows.

  Besides, each row in the source has the following metadata fields as well.
  <table class="table">
  <tr><th>Column</th><th>Type</th></tr>
  <tr>
    <td>`__key`</td>
    <td>Bytes</td>
  </tr>
  <tr>
    <td>`__topic`</td>
    <td>String</td>
  </tr>
  <tr>
    <td>`__messageId`</td>
    <td>Bytes</td>
  </tr>
  <tr>
    <td>`__publishTime`</td>
    <td>Timestamp</td>
  </tr>
  <tr>
    <td>`__eventTime`</td>
    <td>Timestamp</td>
  </tr>
  </table>

  - Example

  The Pulsar topic of AVRO schema s (example 1) converted to a Flink table has the following schema (example 2).

  Example 1

  ```java
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Foo {
      public int i;
      public float f;
      public Bar bar;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Bar {
      public boolean b;
      public String s;
  }

  Schema s = Schema.AVRO(Foo.getClass());
  ```

  ```
  root
   |-- i: INT
   |-- f: FLOAT
   |-- bar: ROW<`b` BOOLEAN, `s` STRING>
   |-- __key: BYTES
   |-- __topic: STRING
   |-- __messageId: BYTES
   |-- __publishTime: TIMESTAMP(3)
   |-- __eventTime: TIMESTAMP(3)
   ```

   Example 2

   The following is the schema of a Pulsar topic with `Schema.DOUBLE`:
   ```
   root
   |-- value: DOUBLE
   |-- __key: BYTES
   |-- __topic: STRING
   |-- __messageId: BYTES
   |-- __publishTime: TIMESTAMP(3)
   |-- __eventTime: TIMESTAMP(3)
   ```

### Pulsar Sources Start Position Configuration

The Flink Pulsar Source allows configuring how the start position for Pulsar
partitions are determined.

Example:

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

FlinkPulsarSource<String> myConsumer = new FlinkPulsarSource<>(...);
myConsumer.setStartFromEarliest();     // start from the earliest record possible
myConsumer.setStartFromLatest();       // start from the latest record (the default behaviour)

DataStream<String> stream = env.addSource(myConsumer);
```

Both `FlinkPulsarSource` and `FlinkPulsarRowSource` have the above explicit configuration methods for start position.


You can also specify the exact offsets the source should start from for each partition:

```java
Map<String, MessageId> offset = new HashMap<>();
offset.put("topic1-partition-0", mid1);
offset.put("topic1-partition-1", mid2);
offset.put("topic1-partition-2", mid3);

myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
```

The above example configures the consumer to start from the specified offsets for
partitions 0, 1, and 2 of topic `topic1`. The offset values should be the
next record that the consumer should read for each partition. Note that
if the consumer needs to read a partition which does not have a specified
offset within the provided offsets map, it will fallback to the default
offsets behaviour (i.e. `setStartLatest()`) for that
particular partition.

Note that these start position configuration methods do not affect the start position when the job is
automatically restored from a failure or manually restored using a savepoint.
On restore, the start position of each Kafka partition is determined by the
offsets stored in the savepoint or checkpoint
(please see the next section for information about checkpointing to enable
fault tolerance for the consumer).

### Pulsar Source and Fault Tolerance

With Flink's checkpointing enabled, the Flink Pulsar Source will consume records from a topic and periodically checkpoint all
its Pulsar offsets, together with the state of other operations, in a consistent manner. In case of a job failure, Flink will restore
the streaming program to the state of the latest checkpoint and re-consume the records from Pulsar, starting from the offsets that were
stored in the checkpoint.

The interval of drawing checkpoints therefore defines how much the program may have to go back at most, in case of a failure.

To use fault tolerant Pulsar Sources, checkpointing of the topology needs to be enabled at the execution environment:

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
```

Also note that Flink can only restart the topology if enough processing slots are available to restart the topology.
So if the topology fails due to loss of a TaskManager, there must still be enough slots available afterwards.
Flink on YARN supports automatic restart of lost YARN containers.


### Pulsar Sources Topic and Partition Discovery

#### Topic/Partition discovery

The Flink Pulsar Source supports discovering dynamically created Pulsar partitions, and consumes them with
exactly-once guarantees. All partitions discovered after the initial retrieval of partition metadata (i.e., when the
job starts running) will be consumed from the earliest possible offset.

By default, partition discovery is disabled. To enable it, set a non-negative value
for `partitionDiscoveryIntervalMillis` in the provided properties config,
representing the discovery interval in milliseconds.

### Pulsar Source and Timestamp Extraction/Watermark Emission

In many scenarios, the timestamp of a record is embedded (explicitly or implicitly) in the record itself.
In addition, the user may want to emit watermarks either periodically, or in an irregular fashion, e.g. based on
special records in the Pulsar stream that contain the current event-time watermark. For these cases, the Flink Pulsar Source allows the specification of an `AssignerWithPeriodicWatermarks` or an `AssignerWithPunctuatedWatermarks`.

Internally, an instance of the assigner is executed per Pulsar partition.
When such an assigner is specified, for each record read from Pulsar, the
`extractTimestamp(T element, long previousElementTimestamp)` is called to assign a timestamp to the record and
the `Watermark getCurrentWatermark()` (for periodic) or the
`Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp)` (for punctuated) is called to determine
if a new watermark should be emitted and with which timestamp.


## Pulsar Sink

Flink’s Pulsar Sink is called `FlinkPulsarSink` for POJO class and `FlinkPulsarRowSink` for Flink Row type.
It allows writing a stream of records to one or more Pulsar topics.

Example:

```java
FlinkPulsarSink<Person> sink = new FlinkPulsarSink(
  serviceUrl,
  adminUrl,
  Optional.of(topic),      // mandatory target topic or use `Optional.empty()` if sink to different topics for each record
  props,
  TopicKeyExtractor.NULL,  // replace this to extract key or topic for each record
  Person.class,
  RecordSchemaType.AVRO);

stream.addSink(sink);
```

### Pulsar Sink and Fault Tolerance

With Flink's checkpointing enabled, the `FlinkPulsarSink` and `FlinkPulsarRowSink`
can provide at-least-once delivery guarantees.

Besides enabling Flink's checkpointing, you should also configure the setter
methods `setLogFailuresOnly(boolean)` and `setFlushOnCheckpoint(boolean)` appropriately.

 * `setFlushOnCheckpoint(boolean)`: by default, this is set to `true`.
 With this enabled, Flink's checkpoints will wait for any
 on-the-fly records at the time of the checkpoint to be acknowledged by Pulsar before
 succeeding the checkpoint. This ensures that all records before the checkpoint have
 been written to Pulsar. This must be enabled for at-least-once.
 
## Advanced Configurations

### Authentication configurations

For Pulsar instance configured with Authentication, Pulsar Flink Connector could be set in similar way with the regular Pulsar Client.

For FlinkPulsarSource, FlinkPulsarRowSource, FlinkPulsarSink and FlinkPulsarRowSink, they all come with a constructor that enables you to
pass in `ClientConfigurationData` as one of the parameters. You should construct a `ClientConfigurationData` first and pass it to the correspond constructor.

For example:

```java

ClientConfigurationData conf = new ClientConfigurationData();
conf.setServiceUrl(serviceUrl);
conf.setAuthPluginClassName(className);
conf.setAuthParams(params);

Properties props = new Properties();
props.setProperty("topic", "test-source-topic");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(adminUrl, conf, new SimpleStringSchema(), props);

```

### Pulsar specific configurations

Client/producer/reader configurations of Pulsar can be set in properties
with `pulsar.client.`/`pulsar.producer.`/`pulsar.reader.` prefix.

Example

`prop.setProperty("pulsar.consumer.ackTimeoutMillis", "10000")`

For possible Pulsar parameters, see
[Pulsar client libraries](https://pulsar.apache.org/docs/en/client-libraries/).

### Use Pulsar Catalog

Flink always searches for tables, views, and UDFs in the current catalog and database. To use Pulsar catalog and treat topics in Pulsar as tables in Flink, you should use `pulsarcatalog` that has been defined in `./conf/sql-client-defaults.yaml`.

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

The following configurations are optional in environment file or can be overridden in a SQL client session using the `SET` command.

<table class="table">

<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>`default-database`</td>
  <td>The default database name.</td>
  <td>public/default</td>
  <td>A topic in Pulsar is treated as a table in Flink when using Pulsar catalog, therefore, `database` is another name for `tenant/namespace`. The database is the basic path for table lookup or creation.</td>
</tr>

<tr>

  <td>`startup-mode`</td>

  <td> The following are valid values:<br>

   * "earliest"(streaming and batch queries)<br>

   * "latest" (streaming query)<br>

  </td>

  <td>"latest"</td>

  <td> `startup-mode` option controls where a table reads data from.
  </td>

</tr>

<tr>

  <td>`table-default-partitions`</td>

  <td> The default number of partitions when a table is created in Table API.

  </td>

  <td>5</td>

  <td>
  A table in Pulsar catalog is a topic in Pulsar, when creating table in Pulsar catalog, `table.partitions` controls the number of partitions when creating a topic.
  </td>

</tr>


</table>

## Build Pulsar Flink Connector
If you want to build a Pulsar Flink connector reading data from Pulsar and writing results to Pulsar, follow the steps below.
1. Check out the source code.
    ```bash
    $ git clone https://github.com/streamnative/pulsar-flink.git
    $ cd pulsar-flink
    ```
2. Install Docker.

    Pulsar-flink connector is using [Testcontainers](https://www.testcontainers.org/) for integration tests. In order to run the integration tests, make sure you have installed [Docker](https://docs.docker.com/docker-for-mac/install/).
3. Set a Java version.

    Change `java.version` and `java.binary.version` in `pom.xml`.
    > #### Note
    > Java version should be consistent with the Java version of flink you use.
4. Build the project.
    ```bash
    $ mvn clean install -DskipTests
    ```
5. Run the tests.
    ```bash
    $ mvn clean install
    ```
Once the installation is finished, there is a fat jar generated under both local maven repo and `target` directory.
