---
layout: page
title: Run Hello-samza in Multi-node YARN
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

You must successfully run the [hello-samza](../../../startup/hello-samza/{{site.version}}/) project in a single-node YARN by following the [hello-samza](../../../startup/hello-samza/{{site.version}}/) tutorial. Now it's time to run the Samza job in a "real" YARN grid (with more than one node).

## Set Up Multi-node YARN

If you already have a multi-node YARN cluster (such as CDH5 cluster), you can skip this set-up section.

### Basic YARN Setting

1\. Download [YARN 2.6](http://mirror.symnds.com/software/Apache/hadoop/common/hadoop-2.6.1/hadoop-2.6.1.tar.gz) to /tmp and untar it.

```bash 
cd /tmp
tar -xvf hadoop-2.6.1.tar.gz
cd hadoop-2.6.1
```

2\. Set up environment variables.

```bash 
export HADOOP_YARN_HOME=$(pwd)
mkdir conf
export HADOOP_CONF_DIR=$HADOOP_YARN_HOME/conf
```

3\. Configure YARN setting file.

```bash 
cp ./etc/hadoop/yarn-site.xml conf
vi conf/yarn-site.xml
```

Add the following property to yarn-site.xml:

```xml 
<property>
    <name>yarn.resourcemanager.hostname</name>
    <!-- hostname that is accessible from all NMs -->
    <value>yourHostname</value>
</property>
```

Download and add capacity-schedule.xml.

```
curl http://svn.apache.org/viewvc/hadoop/common/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/resources/capacity-scheduler.xml?view=co > conf/capacity-scheduler.xml
```

### Set Up Http Filesystem for YARN

The goal of these steps is to configure YARN to read http filesystem because we will use Http server to deploy Samza job package. If you want to use HDFS to deploy Samza job package, you can skip step 4~6 and follow [Deploying a Samza Job from HDFS](deploy-samza-job-from-hdfs.html)

4\. Download Scala package and untar it.

```bash 
cd /tmp
curl http://www.scala-lang.org/files/archive/scala-2.11.8.tgz > scala-2.11.8.tgz
tar -xvf scala-2.11.8.tgz
```

5\. Add Scala, its log jars, and Samza's HttpFileSystem implementation.

```bash 
cp /tmp/scala-2.11.8/lib/scala-compiler.jar $HADOOP_YARN_HOME/share/hadoop/hdfs/lib
cp /tmp/scala-2.11.8/lib/scala-library.jar $HADOOP_YARN_HOME/share/hadoop/hdfs/lib
curl -L http://search.maven.org/remotecontent?filepath=org/clapper/grizzled-slf4j_2.10/1.0.1/grizzled-slf4j_2.10-1.0.1.jar > $HADOOP_YARN_HOME/share/hadoop/hdfs/lib/grizzled-slf4j_2.10-1.0.1.jar
curl -L http://search.maven.org/remotecontent?filepath=org/apache/samza/samza-yarn_2.11/0.12.0/samza-yarn_2.11-0.12.0.jar > $HADOOP_YARN_HOME/share/hadoop/hdfs/lib/samza-yarn_2.11-0.12.0.jar
curl -L http://search.maven.org/remotecontent?filepath=org/apache/samza/samza-core_2.11/0.12.0/samza-core_2.11-0.12.0.jar > $HADOOP_YARN_HOME/share/hadoop/hdfs/lib/samza-core_2.11-0.12.0.jar
```

6\. Add http configuration in core-site.xml (create the core-site.xml file and add content).

```xml 
vi $HADOOP_YARN_HOME/conf/core-site.xml
```

Add the following code:

```xml 
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
      <name>fs.http.impl</name>
      <value>org.apache.samza.util.hadoop.HttpFileSystem</value>
    </property>
</configuration>
```

### Distribute Hadoop File to Slaves

7\. Basically, you copy the hadoop file in your host machine to slave machines. (172.21.100.35, in my case):

```bash 
scp -r . 172.21.100.35:/tmp/hadoop-2.6.1
echo 172.21.100.35 > conf/slaves
sbin/start-yarn.sh
```

* If you get "172.21.100.35: Error: JAVA_HOME is not set and could not be found.", you'll need to add a conf/hadoop-env.sh file to the machine with the failure (172.21.100.35, in this case), which has "export JAVA_HOME=/export/apps/jdk/JDK-1_8_0_45" (or wherever your JAVA_HOME actually is).

8\. Validate that your nodes are up by visiting http://yourHostname:8088/cluster/nodes.

## Deploy Samza Job

Some of the following steps are exactlly identical to what you have seen in [hello-samza](../../../startup/hello-samza/{{site.version}}/). You may skip them if you have already done so.

1\. Download Samza and publish it to Maven local repository.

```bash 
cd /tmp
git clone http://git-wip-us.apache.org/repos/asf/samza.git
cd samza
./gradlew clean publishToMavenLocal
cd ..
```

2\. Download hello-samza project and change the job properties file.

```bash 
git clone git://github.com/linkedin/hello-samza.git
cd hello-samza
vi src/main/config/wikipedia-feed.properties
```

Change the yarn.package.path property to be:

```jproperties 
yarn.package.path=http://yourHostname:8000/target/hello-samza-1.1.0-dist.tar.gz
```

3\. Compile hello-samza.

```bash 
mvn clean package
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-1.1.0-dist.tar.gz -C deploy/samza
```

4\. Deploy Samza job package to Http server..

Open a new terminal, and run:

```bash 
cd /tmp/hello-samza && python -m SimpleHTTPServer
```

Go back to the original terminal (not the one running the HTTP server):

```bash 
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
```

Go to http://yourHostname:8088 and find the wikipedia-feed job. Click on the ApplicationMaster link to see that it's running.

Congratulations! You now run the Samza job in a "real" YARN grid!

