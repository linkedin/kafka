LinkedIn Branch of Apache Kafka
=================

This is the version of Kafka running at LinkedIn.

Kafka was born at LinkedIn. We run thousands of brokers to deliver trillions of
messages per day.  We run a slightly modified version of Apache Kafka trunk.
This branch contains the LinkedIn Kafka release.

This branch is made up of:

* Apache Kafka trunk (upstream) up to some branch point, see *-li* branch name for base version, you'll be able to get the exact commit from git 
* Cherry-picked commits from upstream after branch point
* Patches that are on their way upstream but we have deployed internally in the meantime
* Patches that are of no interest to upstream

We are making this branch available for people interested. We will be
documenting the changes in the near future with some more detailed explanations
in the [LinkedIn Engineering Blog](https://engineering.linkedin.com/blog).

If you are interested in learning more, we invite you to our [Streaming
Meetup](https://www.meetup.com/Stream-Processing-Meetup-LinkedIn/) where we
discuss streaming technologies like [Kafka](http://kafka.apache.org) and
[Samza](http://samza.apache.org).

You are encouraged to check out other Kafka projects from LinkedIn:

* [Cruise Control](https://github.com/linkedin/cruise-control)
* [Li-Apache-Kafka-Clients](https://github.com/linkedin/li-apache-kafka-clients)
* [Burrow](https://github.com/linkedin/Burrow)
* [Kafka Monitor](https://github.com/linkedin/kafka-monitor)

### CI ###
We are currently using Github Actions as the CI framework, and the testing results can be found [here](https://github.com/linkedin/kafka/actions).
To publish a release, go to [the release page](https://github.com/linkedin/kafka/releases) and manually create a new release.
Once the release tag is created, a test job will be triggered to run the necessary tests. And once the test passes, the artifacts
will be published to [the bintray hosting LinkedIn projects](https://dl.bintray.com/linkedin/maven/com/linkedin/kafka/kafka_2.12/).

Currently we've configured the CI flow to run only unit tests for 'clients' and 'core' when a pull request is created or updated:
    ./gradlew :clients:unitTest :core:unitTest
In contrast, all tests for `cliests' and `core' are run when creating a release, which may be significantly longer than running the unit tests:
    ./gradlew :clients:test :core:test
The reason for this mixed approach is to get faster feedback from CI during code reviews
and still gain the more through test coverage when publishing a release.

### Contributing ###

At this moment we are not accepting external contributions directly. Please
contribute to [Apache Kafka](http://kafka.apache.org).

For security issues with this branch please review 
[LinkedIn Security
Guidelines](https://www.linkedin.com/help/linkedin/answer/62924/security-vulnerabilities?lang=en).
General Kafka issues should be communicated via the Kafka community.


Apache Kafka
=================
See our [web site](https://kafka.apache.org) for details on the project.

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

Java 8 should be used for building in order to support both Java 8 and Java 11 at runtime.

Scala 2.12 is used by default, see below for how to use a different Scala version or all of the supported Scala versions.

### Build a jar and run it ###
    ./gradlew jar

Follow instructions in https://kafka.apache.org/documentation.html#quickstart

### Build source jar ###
    ./gradlew srcJar

### Build aggregated javadoc ###
    ./gradlew aggregatedJavadoc

### Build javadoc and scaladoc ###
    ./gradlew javadoc
    ./gradlew javadocJar # builds a javadoc jar for each module
    ./gradlew scaladoc
    ./gradlew scaladocJar # builds a scaladoc jar for each module
    ./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each module

### Run unit/integration tests ###
    ./gradlew test # runs both unit and integration tests
    ./gradlew unitTest
    ./gradlew integrationTest
    
### Force re-running tests without code change ###
    ./gradlew cleanTest test
    ./gradlew cleanTest unitTest
    ./gradlew cleanTest integrationTest

### Running a particular unit/integration test ###
    ./gradlew clients:test --tests RequestResponseTest

### Running a particular test method within a unit/integration test ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testMetadataUpdateWaitTime

### Running a particular unit/integration test with log4j output ###
Change the log4j setting in either `clients/src/test/resources/log4j.properties` or `core/src/test/resources/log4j.properties`

    ./gradlew clients:test --tests RequestResponseTest

### Generating test coverage reports ###
Generate coverage reports for the whole project:

    ./gradlew reportCoverage

Generate coverage for a single module, i.e.: 

    ./gradlew clients:reportCoverage
    
### Building a binary release gzipped tar ball ###
    ./gradlew clean releaseTarGz

The above command will fail if you haven't set up the signing key. To bypass signing the artifact, you can run:

    ./gradlew clean releaseTarGz -x signArchives

The release file can be found inside `./core/build/distributions/`.

### Cleaning the build ###
    ./gradlew clean

### Running a task with one of the Scala versions available (2.11.x, 2.12.x or 2.13.x) ###
*Note that if building the jars with a version other than 2.12.x, you need to set the `SCALA_VERSION` variable or change it in `bin/kafka-run-class.sh` to run the quick start.*

You can pass either the major version (eg 2.12) or the full version (eg 2.12.7):

    ./gradlew -PscalaVersion=2.12 jar
    ./gradlew -PscalaVersion=2.12 test
    ./gradlew -PscalaVersion=2.12 releaseTarGz

### Running a task with all the scala versions enabled by default ###

Append `All` to the task name:

    ./gradlew testAll
    ./gradlew jarAll
    ./gradlew releaseTarGzAll

### Running a task for a specific project ###
This is for `core`, `examples` and `clients`

    ./gradlew core:jar
    ./gradlew core:test

### Listing all gradle tasks ###
    ./gradlew tasks

### Building IDE project ####
*Note that this is not strictly necessary (IntelliJ IDEA has good built-in support for Gradle projects, for example).*

    ./gradlew eclipse
    ./gradlew idea

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory and we don't use Gradle's build directory
to avoid known issues with this configuration.

### Publishing the jar for all version of Scala and for all projects to maven ###
    ./gradlew -Pversion=<release version> uploadArchivesAll

By default, this command will publish artifacts to a Bintray repository named "kafka" under an account specified by the BINTRAY_USER environment variable. The BINTRAY_KEY
environment variable is used for the password for that account.

If you want to override this to use a different maven repository, you should create/update `${GRADLE_USER_HOME}/gradle.properties` (typically, `~/.gradle/gradle.properties`)
and assign the following variables

    mavenUrl=
    mavenUsername=
    mavenPassword=

Signing is disabled by default. If you need signing, please set the following variables in `gradle.properties` as well:

    signing.keyId=
    signing.password=
    signing.secretKeyRingFile=

### Publishing the streams quickstart archetype artifact to maven ###
For the Streams archetype project, one cannot use gradle to upload to maven; instead the `mvn deploy` command needs to be called at the quickstart folder:

    cd streams/quickstart
    mvn deploy

Please note for this to work you should create/update user maven settings (typically, `${USER_HOME}/.m2/settings.xml`) to assign the following variables

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...


### Installing the jars to the local Maven repository ###
    ./gradlew installAll

### Building the test jar ###
    ./gradlew testJar

### Determining how transitive dependencies are added ###
    ./gradlew core:dependencies --configuration runtime

### Determining if any dependencies could be updated ###
    ./gradlew dependencyUpdates

### Running code quality checks ###
There are two code quality analysis tools that we regularly run, spotbugs and checkstyle.

#### Checkstyle ####
Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

    ./gradlew checkstyleMain checkstyleTest

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also are printed to the console. The build will fail if Checkstyle fails.

#### Spotbugs ####
Spotbugs uses static analysis to look for bugs in the code.
You can run spotbugs using:

    ./gradlew spotbugsMain spotbugsTest -x test

The spotbugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: limits the maximum number of processes for each task.
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for spotBugs. This also disables HTML reports as only one can be enabled at a time.

### Dependency Analysis ###

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtime --dependency com.fasterxml.jackson.core:jackson-databind

These take the same arguments as the builtin variants.

### Running system tests ###

See [tests/README.md](tests/README.md).

### Running in Vagrant ###

See [vagrant/README.md](vagrant/README.md).

### Contribution ###

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html
