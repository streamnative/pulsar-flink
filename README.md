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
          <excludes>
            <exclude>com.google.code.findbugs:jsr305</exclude>
            <exclude>org.slf4j:*</exclude>
            <exclude>log4j:*</exclude>
          </excludes>
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
  -c com.example.entry.point.ClassName
  --classpath file://path/to/jars/your_fat_jar.jar
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

## Usage

### Read data from Pulsar

#### Create a Pulsar source for streaming queries
The following examples are in Scala.

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val props = new Properties()
props.setProperty("service.url", "pulsar://...")
props.setProperty("admin.url", "http://...")
props.setProperty("partitionDiscoveryIntervalMillis", "5000")
props.setProperty("startingOffsets", "earliest")
props.setProperty("topic", "test-source-topic")
val source = new FlinkPulsarSource(props)
// you don't need to provide a type information to addSource since FlinkPulsarSource is ResultTypeQueryable
val dataStream = env.addSource(source)(null)

// chain operations on dataStream of Row and sink the output
// end method chaining

env.execute()
```

#### Register topics in Pulsar as streaming tables
The following examples are in Scala.

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)
val props = new Properties()
props.setProperty("service.url", "pulsar://...")
props.setProperty("admin.url", "http://...")
props.setProperty("partitionDiscoveryIntervalMillis", "5000")
props.setProperty("startingOffsets", "earliest")
props.setProperty("topic", "test-source-topic")
tEnv
  .connect(new Pulsar().properties(props))
  .inAppendMode()
  .registerTableSource("pulsar-test-table")
```

The following options must be set for the Pulsar source.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Description</th></tr>
<tr>
  <td>`topic`</td>
  <td>A topic name string</td>
  <td>The topic to be consumed.
  Only one of `topic`, `topics` or `topicsPattern`
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>`topics`</td>
  <td>A comma-separated list of topics</td>
  <td>The topic list to be consumed.
  Only one of `topic`, `topics` or `topicsPattern`
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>`topicsPattern`</td>
  <td>A Java regex string</td>
  <td>The pattern used to subscribe to topic(s).
  Only one of `topic`, `topics` or `topicsPattern`
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>`service.url`</td>
  <td>A service URL of your Pulsar cluster</td>
  <td>The Pulsar `serviceUrl` configuration.</td>
</tr>
<tr>
  <td>`admin.url`</td>
  <td>A service HTTP URL of your Pulsar cluster</td>
  <td>The Pulsar `serviceHttpUrl` configuration.</td>
</tr>
</table>

The following configurations are optional.

<table class="table">

<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr>

<tr>

  <td>`startingOffsets`</td>

  <td>The following are valid values:<br>

  * "earliest"(streaming and batch queries)<br>

  * "latest" (streaming query)<br>

  * A JSON string<br>

    **Example**<br>

    """ {"topic-1":[8,11,16,101,24,1,32,1],"topic-5":[8,15,16,105,24,5,32,5]} """
  </td>

  <td>"latest"</td>


  <td>

  `startingOffsets` option controls where a consumer reads data from.

  * "earliest": lacks a valid offset, the consumer reads all the data in the partition, starting from the very beginning.<br>

*  "latest": lacks a valid offset, the consumer reads from the newest records written after the consumer starts running.<br>

* A JSON string: specifies a starting offset for each Topic. <br>
You can use `org.apache.flink.pulsar.JsonUtils.topicOffsets(Map[String, MessageId])` to convert a message offset to a JSON string. <br>

**Note**: <br>

* "latest" only applies when a new query is started, and the resuming will
  always pick up from where the query left off. Newly discovered partitions during a query will start at
  "earliest".</td>

</tr>

<tr>

  <td>`partitionDiscoveryIntervalMillis`</td>

  <td> A long value or a string which can be converted to long

  </td>

  <td>-1</td>

  <td>

  `partitionDiscoveryIntervalMillis` option controls whether the source discovers newly added topics or partitions match the topic options
  while executing the streaming job.
  A positive long `l` would trigger the discoverer run every `l` milliseconds,
  and negative values would turn off a topic or a partition discoverer. <br>

</tr>

</table>

#### Schema of Pulsar source
* For topics without schema or with primitive schema in Pulsar, messages' payload
is loaded to a `value` column with the corresponding type with Pulsar schema.

* For topics with Avro or JSON schema, their field names and field types are kept in the result rows.

Besides, each row in the source has the following metadata fields as well.
<table class="table">
<tr><th>Column</th><th>Type</th></tr>
<tr>
  <td>`__key`</td>
  <td>Binary</td>
</tr>
<tr>
  <td>`__topic`</td>
  <td>String</td>
</tr>
<tr>
  <td>`__messageId`</td>
  <td>Binary</td>
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

**Example**

The Pulsar topic of AVRO schema s (example 1) converted to a Flink table has the following schema (example 2).

Example 1

```scala
  case class Foo(i: Int, f: Float, bar: Bar)
  case class Bar(b: Boolean, s: String)
  val s = Schema.AVRO(Foo.getClass)
```

Example 2

```
root
 |-- i: integer (nullable = false)
 |-- f: float (nullable = false)
 |-- bar: struct (nullable = true)
 |    |-- b: boolean (nullable = false)
 |    |-- s: string (nullable = true)
 |-- __key: binary (nullable = true)
 |-- __topic: string (nullable = true)
 |-- __messageId: binary (nullable = true)
 |-- __publishTime: timestamp (nullable = true)
 |-- __eventTime: timestamp (nullable = true)
 ```

 The following is the schema of a Pulsar topic with `Schema.DOUBLE`:
 ```
 root
 |-- value: double (nullable = false)
 |-- __key: binary (nullable = true)
 |-- __topic: string (nullable = true)
 |-- __messageId: binary (nullable = true)
 |-- __publishTime: timestamp (nullable = true)
 |-- __eventTime: timestamp (nullable = true)
 ```

### Write data to Pulsar

The DataStream written to Pulsar can have an arbitrary type.

For DataStream[Row], `__topic` field is used to identify the topic this message will be sent to, `__key` is encoded as metadata of Pulsar message, and all the other fields are grouped and encoded using AVRO and put in `value()`:

```scala
producer.newMessage().key(__key).value(avro_encoded_fields)
```
For DataStream[T] where T is a POJO type, each record in data stream will be encoded using AVRO and put in Pulsar messages `value()`, optionally, you could provide an extra `topicKeyExtractor` that identify topic and key for each record.

#### Create a Pulsar sink for streaming queries
The following examples are in Scala.
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val stream = .....

val prop = new Properties()
prop.setProperty("service.url", serviceUrl)
prop.setProperty("admin.url", adminUrl)
prop.setProperty("flushOnCheckpoint", "true")
prop.setProperty("failOnWrite", "true")
props.setProperty("topic", "test-sink-topic")

stream.addSink(new FlinkPulsarSink(prop, DummyTopicKeyExtractor))
env.execute()
```

#### Write a streaming table to Pulsar
The following examples are in Scala.
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

val prop = new Properties()
prop.setProperty("service.url", serviceUrl)
prop.setProperty("admin.url", adminUrl)
prop.setProperty("flushOnCheckpoint", "true")
prop.setProperty("failOnWrite", "true")
props.setProperty("topic", "test-sink-topic")

tEnv
  .connect(new Pulsar().properties(props))
  .inAppendMode()
  .registerTableSource("sink-table")

val sql = "INSERT INTO sink-table ....."
tEnv.sqlUpdate(sql)
env.execute()
```

The following options must be set for a Pulsar sink.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Description</th></tr>
<tr>
  <td>`service.url`</td>
  <td>A service URL of your Pulsar cluster</td>
  <td>The Pulsar `serviceUrl` configuration.</td>
</tr>
<tr>
  <td>`admin.url`</td>
  <td>A service HTTP URL of your Pulsar cluster</td>
  <td>The Pulsar `serviceHttpUrl` configuration.</td>
</tr>
</table>

The following configurations are optional.

<table class="table">

<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>`topic`</td>
  <td>A topic name string</td>
  <td></td>
  <td>The topic to be write to. If this option is not set, DataStreams or tables write to Pulsar must contain a TopicKeyExtractor that return nonNull topics or `__topic` field.</td>
</tr>

<tr>

  <td>`flushOnCheckpoint`</td>

  <td> Whether flush all records write until checkpoint and wait for confirms.

  </td>

  <td>true</td>

  <td>

  At-least-once semantic is achieved when `flushOnCheckpoint` is set to `true` and checkpoint is enabled on execution environment. Otherwise, you get no write guarantee.<br>
  </td>

</tr>

<tr>

  <td>`failOnWrite`</td>

  <td> Whether fail the sink while sending records to Pulsar fail.

  </td>

  <td>false</td>

  <td>
  None
  </td>

</tr>


</table>

#### Limitations

Currently, we provide at-least-once semantic when `flushOnCheckpoint` is set to `true`. Consequently, when writing streams to Pulsar, some records may be duplicated.
We would provide exactly-once sink semantic when Pulsar has transaction supports.

### Pulsar specific configurations

Client/producer/consumer configurations of Pulsar can be set in properties
with `pulsar.client.`/`pulsar.producer.`/`pulsar.consumer.` prefix.

Example

`prop.setProperty("pulsar.consumer.ackTimeoutMillis", "10000")`

For possible Pulsar parameters, see
[Pulsar client libraries](https://pulsar.apache.org/docs/en/client-libraries/).

## Build Pulsar Flink Connector
If you want to build a Pulsar Flink connector reading data from Pulsar and writing results to Pulsar, follow the steps below.

1. Check out the source code.

```bash
$ git clone https://github.com/streamnative/pulsar-flink.git
$ cd pulsar-flink
```

2. Install Docker.

Pulsar-flink connector is using [Testcontainers](https://www.testcontainers.org/) for
integration tests. In order to run the integration tests, make sure you
have installed [Docker](https://docs.docker.com/docker-for-mac/install/).

3. Set a Scala version.
Change `scala.version` and `scala.binary.version` in `pom.xml`.
> #### Note
> Scala version should be consistent with the Scala version of flink you use.

4. Build the project.

```bash
$ mvn clean install -DskipTests
```

5. Run the tests.

```bash
$ mvn clean install
```
Once the installation is finished, there is a fat jar generated under both local maven repo and `target` directory.
