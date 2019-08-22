# pulsar-flink
Elastic data processing with [Apache Pulsar](https://pulsar.apache.org) and [Apache Flink](https://flink.apache.org).

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
If you have already build an fat jar using the shade maven plugin above, your jar can be added to `flink run` using `--classpath`.

> #### Note
> the paths must specify a protocol (e.g. file://) and be accessible on all nodes.

Example

```
$ ./bin/flink run
  -c com.example.entry.point.ClassName
  --classpath file://path/to/jars/your_fat_jar.jar
  ...
```

#### Scala REPL  
For experimenting on the interactive Scala shell `bin/start-scala-shell.sh`, you can use `--addclasspath` to add `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_SPARK_VERSION}}.jar` directly.

Example

```
$ ./bin/start-scala-shell.sh remote <hostname> <portnumber>
  --addclasspath pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_SPARK_VERSION}}.jar
```
For more information about **submitting applications with CLI**, see [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/cli.html).

