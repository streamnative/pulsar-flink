# pulsar-flink
The Pulsar Flink connector implements the function of elastic data processing using [Apache Pulsar](https://pulsar.apache.org) and [Apache Flink](https://flink.apache.org).

Chinese document: [README_CN](doc/README_CN.md)

>Note: The pulsar-flink connector uses a multi-branch approach for Flink versions. The current master branch is based on Flink 1.11. flink 1.9 is maintained in the branch flink-1.9.

## Prerequisites

- Java 8 or higher
- Flink 1.9.0 or higher
- Pulsar 2.4.0 or higher

## Basic

### Link

#### Client Lib
For projects using SBT, Maven, and Gradle, you can use the following parameters to set to your project.

- `FLINK_VERSION` parameter now has `1.9` and `1.11` options. This branch use flink-1.11 version.
  - Version 1.9 supports flink 1.9-1.10
  - 1.11 version supports 1.11+
- The `SCALA_BINARY_VERSION` parameter is related to the Scala version used by flink. There are `2.11` and `2.12` options available.
- `PULSAR_FLINK_VERSION` is the version of this connector.

```
    groupId = io.streamnative.connectors
    artifactId = pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{FLINK_VERSION}}
    version = {{PULSAR_FLINK_VERSION}}
```
The Jar package is located in [Bintray Maven repository of StreamNative](https://dl.bintray.com/streamnative/maven).


Maven project can add warehouse configuration to your `pom.xml`, the content is as follows:

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
The Gradle project can add warehouse configuration in `build.gradle`, the content is as follows:

```groovy
repositories {
        maven {
            url'https://dl.bintray.com/streamnative/maven'
        }
}
```

For maven projects, to build an application JAR that contains all the dependencies required by the library and the pulsar flink connector, you can use the following shade plugin definition template:

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

For a Gradle project, to build an application JAR containing all the dependencies required by the library and the pulsar flink connector, you can use the following [shade](https://imperceptiblethoughts.com/shadow/) plugin definition template:

```groovy
buildscript {
    repositories {
        jcenter()
    }
    dependencies {
        classpath'com.github.jengelman.gradle.plugins:shadow:6.0.0'
    }
}

apply plugin:'com.github.johnrengelman.shadow'
apply plugin:'java'
```





### Deployment

#### Client library
Like any Flink application, `./bin/flink run` is used to compile and start your application.

If you have built a jar containing dependencies using the shade plugin above, you can use `--classpath` to add your jar to `flink run`.

> #### Note
> The format of the path must be a protocol (for example, `file://`), and the path is accessible on all nodes.

Example:
```
$ ./bin/flink run
  -c com.example.entry.point.ClassName file://path/to/jars/your_fat_jar.jar
  ...
```

#### Scala REPL
Try `bin/start-scala-shell.sh` in the interactive Scala shell, you can use the `--addclasspath` parameter to directly add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}. jar`.

Example:
```
$ ./bin/start-scala-shell.sh remote <hostname> <portnumber>
  --addclasspath pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
For more information about submitting applications using CLI, please refer to [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/cli.html).


#### SQL Client
To use [SQL Client Beta](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sqlClient.html) and write SQL queries to manipulate the data in Pulsar, you can Use the `--addclasspath` parameter to directly add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`.

Example:
```
$ ./bin/sql-client.sh embedded --jar pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
By default, to use the Pulsar directory in SQL Client and automatically register it at startup, SQL Client will read its configuration from the environment file `./conf/sql-client-defaults.yaml`. You need to add the Pulsar catalog in the `catalogs` section of this YAML file:

```yaml
catalogs:
-name: pulsarcatalog
    type: pulsar
    default-database: tn/ns
    service-url: "pulsar://localhost:6650"
    admin-url: "http://localhost:8080"
    format.type: "json"
```



## Stream Environment

### Source

Flink's Pulsar consumer is called `FlinkPulsarSource<T>` It provides access to one or more Pulsar topics.

The construction method has the following parameters:

1. Connect the service address `serviceUrl` used by the Pulsar instance and the management address `adminUrl`.
2. When using `FlinkPulsarSource`, you need to set `PulsarDeserializationSchema<T>`.
3. The Properties parameter is used to configure the behavior of the Pulsar Consumer.
   The required parameters for Properties are as follows:

    -Among these parameters, `topic`, `topics`, or `topicsPattern` must have an existing value, and there can only be one. Used to configure the topic information for consumption. (**`topics` is multiple topics separated by commas`, `, `topicsPattern` is a java regular expression that can match several topics**)
   
      The consumption mode can be set by FlinkPulsarSource's setStartFromLatest, setStartFromEarliest, setStartFromSpecificOffsets, setStartFromSubscription, etc.


example:

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic");
props.setProperty("partitiondiscoveryintervalmillis", "5000");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, new PulsarDeserializationSchemaWrapper(new SimpleStringSchema()), props);

// or setStartFromLatest, setStartFromSpecificOffsets, setStartFromSubscription
source.setStartFromEarliest();

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```



#### Pulsar Source and Timestamp Extraction/Watermark Emission

In many cases, the timestamp of the record (explicitly or implicitly) is embedded in the record itself. Also, users may want to issue watermarks periodically or irregularly. Based on a special record in the Pulsar stream containing the current event time watermark. For these cases, the Flink Pulsar source allows specifying `AssignerWithPeriodicWatermarks` or `AssignerWithPunctuatedWatermarks`.

Internally, each Pulsar partition executes an instance of the allocator. After specifying such an allocator, for each record read from Pulsar, call `extractTimestamp (T element, long previousElementTimestamp)` to assign a timestamp to the record, and then `Watermark getCurrentWatermark()` (for periodic) or call watermark `checkAndGetNextWatermark(T lastElement, long extractTimestamp)` (for punctuation) determines whether a new watermark should be issued and which timestamp to use.

### Sink

Use `FlinkPulsarSink` instance to handle the POJO type or `FlinkPulsarRowSink` to handle Flink Row type.
It allows the record stream to be written to one or more Pulsar topics.

Example:

```java
        PulsarSerializationSchema<Person> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(
                (SerializationSchema<Person>) element -> { 
                    JSONSchema<Person> jsonSchema = JSONSchema.of(Person.class);
                    return jsonSchema.encode(element); 
                })
                .usePojoMode(Person.class, RecordSchemaType.JSON)
                .build();
        new FlinkPulsarSink(
                serviceUrl,
                adminUrl,
                Optional.of(topic),
                getSinkProperties(),
                pulsarSerialization);
stream.addSink(sink);
```

`PulsarSerializationSchema` is a wrapper for Flink `SerializationSchema`, providing more functionality. Most of the time, you don't need to implement `PulsarSerializationSchema` by yourself, we provide `PulsarSerializationSchemaWrapper` to wrap a Flink `SerializationSchema` to become a `PulsarSerializationSchema`.

`PulsarSerializationSchema` uses the builder pattern, you can call `setKeyExtractor` or `setTopicExtractor` to satisfy the need for extracting keys from each message and customizing the target topic.

In particular, since Pulsar maintains its own Schema information internally, our messages must be able to derive a SchemaInfo when written to Pulsar. 
The `useSpecialMode`, `useAtomicMode`, `usePojoMode`, and `useRowMode` method can help you quickly build the Schema information you need for Pulsar. You must choose just one of these four modes.

SpecialMode: Directly specifies the `Schema<?> schema` in Pulsar.

AtomicMode: For some data of atomic type, pass the type of AtomicDataType, such as `DataTypes.INT()`, which will correspond to the `Schema<Integer>` in Pulsar.

PojoMode: You need to pass a custom Class object and one of Json or Arvo to specify the way to build the composite type Schema. For example, the
`usePojoMode(Person.class, RecordSchemaType.JSON)`

RowMode: In general, you will not use this mode, it is used for our internal implementation of the Table&SQL API.


#### Fault tolerance

After enabling Flink checkpoints, `FlinkPulsarSink` can provide an at-least-once delivery guarantee.

In addition to enabling Flink checkpointing, you should also configure `setLogFailuresOnly(boolean)` and `setFlushOnCheckpoint(boolean)`.

  *`setFlushOnCheckpoint(boolean)`*: By default, it is set to `true`. After enabling this function, writing pulsar records will be executed at this checkpoint snapshotState. This ensures that all records before checkpoint are written to the pulsar. Note that the at-least-once setting of flink must be turned on at the same time.

## Table Environment



The Pulsar-flink connector supports Flink's Table comprehensively, covering the following list:

- Connector
- Catalog
- SQL, DDL (DDL is supported in flink 1.11)



### Connector

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
see.setParallelism(1);
StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

String topic = "";
String tableName = "pulsarTable";

TableSchema tSchema = TableSchema.builder().field("value", DataTypes.BOOLEAN()).build();
tEnv.connect(
  new Pulsar()
                .urls(getServiceUrl(), getAdminUrl())
                .topic(topic)
                .startFromEarliest()
                .property(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "5000")
)
        .withSchema(new Schema().schema(tSchema))
        .inAppendMode()
        .createTemporaryTable(tableName);
Table t = tEnv.sqlQuery("select `value` from "+ tableName);
```

### Catalog

Flink always searches tables, views, and UDFs in the current directory and database. To use Pulsar Catalog and treat topics in Pulsar as tables in Flink, you should use the `pulsarcatalog` defined in `./conf/sql-client-defaults.yaml`.

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

The following configuration is optional in the environment file or can be overridden in the SQL client session using the `SET` command.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>`default-database`</td>
  <td>The default database name. </td>
  <td>public/default</td>
  <td>When using Pulsar catalog, topics in Pulsar are regarded as tables in Flink, therefore, `database` is another name for `tenant/namespace`. The database is the basic path for table lookup or creation. </td>
</tr>


Note: Because the delete operation is dangerous, deleting `tenant/namespace` and `Flink` in the catalog is not supported.



### SQL, DDL



```sql
set global.disable.operator.chain = true;

create table test_flink_sql(
    `rip` VARCHAR,
    `rtime` VARCHAR,
    `uid` bigint,
    `client_ip` VARCHAR,
    `day` as TO_DATE(rtime),
    `hour` as date_format(rtime,'HH')
) with (
    'connector.type' ='pulsar',
    'connector.version' = '1',
    'connector.topic' ='persistent://public/default/test_flink_sql',
    'connector.service-url' ='pulsar://xxx',
    'connector.admin-url' ='http://xxx',
    'connector.startup-mode' ='earliest',
    'format.type' ='json',
    'update-mode' ='append'
);

insert into hive.test.test_flink_sql
select
rip, rtime,
if (uid is null, 0, uid) as uid,
if (client_ip is null,'', client_ip) as client_ip,
cast(`day` as string) as `day`,
cast(`hour` as string) as `hour`
from test_flink_sql;
```



DDL is supported in the flink 1.11 package, and more detailed parameters can be set in the creation table.

## DeserializationSchema

DeserializationSchema is used to decode Source records. The core method can only decode pulsar Message#value. In a custom scenario, users need to get more information from the Message, which cannot be satisfied.

The pulsar-flink connector does not directly use `DeserializationSchema`, but defines `PulsarDeserializationSchema<T>`. Through the `PulsarDeserializationSchema<T>` instance, it leaves more room for users to expand.

Use `new PulsarDeserializationSchemaWrapper<>(deserializationSchema)` to support instances of `DeserializationSchema`.

The pulsar-flink connector provides two `DeserializationSchema` decoders:

1. `JsonDeser`: When using JSONSchema for pulsar topic, you can create a `DeserializationSchema` instance through `JsonDeser.of(POJO_CLASS_NAME.class)`.

2. `AvroDeser`: When using AVROSchema for pulsar topic, you can use `AvroDeser.of(POJO_CLASS_NAME.class)` for `DeserializationSchema` instance.



## Advanced configuration

### Configuration parameters



| Parameters | Default Value | Description | Effective Range |
| ------------------------------------ | ------------- | ------------------------------------------------------------ | ------------ |
| topic | null | pulsar topic | source |
| topics | null | Multiple pulsar topics connected by half-width commas | source |
| topicspattern | null | Multiple pulsar topics with more java regular matching | source |
| partitiondiscoveryintervalmillis | -1 | Automatically discover increase and decrease topics, milliseconds. -1 means not open. | source |
| clientcachesize | 5 | Number of cached pulsar clients | source, sink |
| auth-params | null | pulsar clients auth| source, sink |
| auth-plugin-classname | null | pulsar clients auth | source, sink |
| flushoncheckpoint | true | Write a message to pulsar | sink | during flink snapshotState
| failonwrite | false | When sink error occurs, continue to confirm the message | sink |
| polltimeoutms | 120000 | The timeout period for waiting to get the next message, milliseconds | source |
| failondataloss | true | Does it fail when data is lost | source |
| commitmaxretries | 3 | Maximum number of retries when offset to pulsar message | source |
| startup-mode | null | earliest, latest, the position where subscribers consume news, required | catalog |
| table-default-partitions | 5 | Specify the number of partitions to create a topic | catalog |
| pulsar.reader.* | | For detailed configuration of pulsar consumer, please refer to [pulsar reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader) | source |
| pulsar.reader.subscriptionRolePrefix | flink-pulsar- | When no subscriber is specified, the prefix of the subscriber name is automatically created | source |
| pulsar.reader.receiverQueueSize | 1000 | Receive queue size | source |
| pulsar.producer.* | | For detailed configuration of pulsar consumer, please refer to [pulsar producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer) | Sink |
| pulsar.producer.sendTimeoutMs | 30000 | Timeout time when sending a message, milliseconds | Sink |
| pulsar.producer.blockIfQueueFull | false | Producer writes messages. When the queue is full, block the method instead of throwing an exception | Sink |

`pulsar.reader.*` and `pulsar.producer.*` specify a more detailed configuration of pulsar behavior, * replace with the configuration name in pulsar, and the content refers to the link in the table.



In the DDL statement, the format of the above parameters is used and adjusted,

Configure the setting of `pulsar.reader.readerName=test_flink_sql_v1`

```
'connector.properties.0.key' ='pulsar.reader.readerName', //parameter name
'connector.properties.0.value' ='test_flink_sql_v1', // parameter value
```

Example：

```sql
create table test_flink_sql(
   `data` VARCHAR
) with (
   'connector.type' = 'pulsar',
   'connector.version' = '1',
   'connector.topic' = 'persistent://public/default/test_flink_sql',
   'connector.service-url' = 'pulsar://xxx',
   'connector.admin-url' = 'http://xxx',
   'connector.startup-mode' = 'earliest',
   'connector.properties.0.key' = 'pulsar.reader.readerName', //param name
   'connector.properties.0.value' = 'test_flink_sql_v1',      // param value
   'connector.properties.1.key' = 'pulsar.reader.subscriptionRolePrefix',
   'connector.properties.1.value' = 'test_flink_sql_v1',
   'connector.properties.2.key' = 'pulsar.reader.receiverQueueSize',
   'connector.properties.2.value' = '1000',
   'connector.properties.3.key' = 'partitiondiscoveryintervalmillis', 
   'connector.properties.3.value' = '5000',                           
   'format.type' = 'json',
   'update-mode' = 'append'
);
```


### Authentication configurations

For Pulsar instance configured with Authentication, Pulsar Flink Connector could be set similarly with the regular Pulsar Client.

For FlinkPulsarSource and FlinkPulsarSink, they all come with a constructor that enables you to
pass in `ClientConfigurationData` as one of the parameters. You should construct a `ClientConfigurationData` first and pass it to the correspond constructor.

For example:

```java

ClientConfigurationData conf = new ClientConfigurationData();
conf.setServiceUrl(serviceUrl);
conf.setAuthPluginClassName(className);
conf.setAuthParams(params);

Properties props = new Properties();
props.setProperty("topic", "test-source-topic");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(adminUrl, conf, new PulsarDeserializationSchemaWrapper(new SimpleStringSchema()), props);

```

## Build Pulsar Flink Connector
If you want to build a Pulsar Flink connector reading data from Pulsar and writing results to Pulsar, follow the steps below.
1. Check out the source code.
    ```bash
    $ git clone https://github.com/streamnative/pulsar-flink.git
    $ cd pulsar-flink
    ```
2. Install Docker.

    Pulsar-flink connector is using [Testcontainers](https://www.testcontainers.org/) for integration tests. To run the integration tests, make sure you have installed [Docker](https://docs.docker.com/docker-for-mac/install/).
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
