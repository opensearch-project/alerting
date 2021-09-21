- [Developer Guide](#developer-guide)
  - [Forking and Cloning](#forking-and-cloning)
  - [Install Prerequisites](#install-prerequisites)
    - [JDK 14](#jdk-14)
  - [Setup](#setup)
  - [Build](#build)
    - [Building from the command line](#building-from-the-command-line)
    - [Run integration tests with Security enabled](#run-integration-tests-with-security-enabled)
    - [Building from the IDE](#building-from-the-ide)
    - [Debugging](#debugging)
    - [Advanced: Launching multi node clusters locally](#advanced-launching-multi-node-clusters-locally)

## Developer Guide

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

#### JDK 14

OpenSearch components build using Java 14 at a minimum. This means you must have a JDK 14 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 14 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-14`.

### Setup

1. Clone the repository (see [Forking and Cloning](#forking-and-cloning))
2. Make sure `JAVA_HOME` is pointing to a Java 14 JDK (see [Install Prerequisites](#install-prerequisites))
3. Launch Intellij IDEA, Choose Import Project and select the settings.gradle file in the root of this package.

### Build

This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build. we also use the OpenSearch build tools for Gradle. These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in this package will work the way it's described in the Gradle documentation. If you encounter such a situation, the OpenSearch build tools [source code](https://github.com/opensearch-project/OpenSearch/tree/main/buildSrc/src/main/groovy/org/opensearch/gradle) is your best bet for figuring out what's going on.

Currently we just put RCF jar in lib as dependency. Plan to publish to Maven and we can import it later. Before publishing to Maven, you can still build this package directly and find source code in RCF Github package.

#### Building from the command line

1. `./gradlew build` builds and tests all subprojects.
2. `./gradlew :alerting:run` launches a single node cluster with the alerting plugin installed.
3. `./gradlew :alerting:run -PnumNodes=3` launches a multi-node cluster with the alerting plugin installed.
4. `./gradlew :alerting:integTest` launches a single node cluster with the alerting plugin installed and runs all integ tests.
5. `./gradlew :alerting:integTest -PnumNodes=3` launches a multi-node cluster with the alerting plugin installed and runs all integ tests.
6. `./gradlew :alerting:integTest -Dtests.class="*MonitorRunnerIT"` runs a single integ test class
7. `./gradlew :alerting:integTest -Dtests.method="test execute monitor with dryrun"` runs a single integ test method
 (remember to quote the test method name if it contains spaces).

When launching a cluster using one of the above commands, logs are placed in `alerting/build/testclusters/integTest-0/logs/`. Though the logs are teed to the console, in practices it's best to check the actual log file.

#### Run integration tests with Security enabled 

1. Setup a local opensearch cluster with security plugin.

   - `./gradlew :alerting:integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=es-integrationtest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin`

   - `./gradlew :alerting:integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=es-integrationtest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin --tests "org.opensearch.alerting.MonitorRunnerIT.test execute monitor returns search result"`

#### Run integration tests with Notification plugin 

1. Setup a local opensearch cluster with notification plugin.

   - `./gradlew :alerting:integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=es-integrationtest`

   - `./gradlew :alerting:integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=es-integrationtest --tests "org.opensearch.alerting.MonitorRunnerIT.test execute monitor returns search result"`

#### Run integration tests with Notification plugin and Security enabled

1. Setup a local opensearch cluster with notification and security plugin.

   - `./gradlew :alerting:integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=es-integrationtest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin`

   - `./gradlew :alerting:integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=es-integrationtest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin --tests "org.opensearch.alerting.MonitorRunnerIT.test execute monitor returns search result"`

#### Building from the IDE

Currently, the only IDE we support is IntelliJ IDEA.  It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar and the extra parameters can be passed in via the Launch Configurations VM arguments. 

#### Debugging

Sometimes it's useful to attach a debugger to either the Opensearch cluster or the integ tests to see what's going on. When running unit tests, hit **Debug** from the IDE's gutter to debug the tests.
You must start your debugger to listen for remote JVM before running the below commands.

To debug code running in an actual server, run:

```
./gradlew :alerting:integTest -Dcluster.debug # to start a cluster and run integ tests
```

OR

```
./gradlew :alerting:run --debug-jvm # to just start a cluster that can be debugged
```

The Opensearch server JVM will launch suspended and wait for a debugger to attach to `localhost:5005` before starting the Opensearch server. The IDE needs to listen for the remote JVM. If using Intellij you must set your debug configuration to "Listen to remote JVM" and make sure "Auto Restart" is checked. You must start your debugger to listen for remote JVM before running the commands.

To debug code running in an integ test (which exercises the server from a separate JVM), run:

```
./gradlew :alerting:integTest -Dtest.debug 
```

The test runner JVM will start suspended and wait for a debugger to attach to `localhost:8000` before running the tests.

Additionally, it is possible to attach one debugger to the cluster JVM and another debugger to the test runner. First, make sure one debugger is listening on port `5005` and the other is listening on port `8000`. Then, run:
```
./gradlew :alerting:integTest -Dtest.debug -Dcluster.debug
```

### Advanced: Launching multi-node clusters locally

Sometimes you need to launch a cluster with more than one Opensearch server process.

You can do this by running `./gradlew :alerting:run -PnumNodes=<numberOfNodesYouWant>`

You can also run the integration tests against a multi-node cluster by running `./gradlew :alerting:integTest -PnumNodes=<numberOfNodesYouWant>`

You can also debug a multi-node cluster, by using a combination of above multi-node and debug steps.
But, you must set up debugger configurations to listen on each port starting from `5005` and increasing by 1 for each node.  
