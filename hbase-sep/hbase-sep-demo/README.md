# HBase-SEP Demo Project 

This project contains some sample code making use of the SEP. It demonstrates a LoggingConsumer that reports mutation events that occur in HBase.

## Setup

For the purpose of this demo, we will run HBase in standalone mode (zookeeper is embbeded,
no hdfs is used). Note that by default, HBase stores its data in /tmp, which will be lost,
see the HBase docs on how to change this.

### Download HBase

Download HBase (0.94) from [http://hbase.apache.org/](http://hbase.apache.org/).

### Configure HBase

Edit conf/hbase-site.xml and add the following configuration:

    <configuration>
      <!-- SEP is basically replication, so enable it -->
      <property>
        <name>hbase.replication</name>
        <value>true</value>
      </property>
      <!-- Source ratio of 100% makes sure that each SEP consumer is actually
           used (otherwise, some can sit idle, especially with small clusters) -->
      <property>
        <name>replication.source.ratio</name>
        <value>1.0</value>
      </property>
      <!-- Maximum number of hlog entries to replicate in one go. If this is
           large, and a consumer takes a while to process the events, the
           HBase rpc call will time out. -->
      <property>
        <name>replication.source.nb.capacity</name>
        <value>1000</value>
      </property>
      <!-- A custom replication source that fixes a few things and adds
           some functionality (doesn't interfere with normal replication
           usage). -->
      <property>
        <name>replication.replicationsource.implementation</name>
        <value>com.ngdata.sep.impl.SepReplicationSource</value>
      </property>
    </configuration>

### Add sep libraries to HBase:

This makes available the SepReplicationSource to HBase.

    cp hbase-sep/impl/target/hbase-sep-impl-1.0.jar hbase/lib/
    cp hbase-sep/api/target/hbase-sep-api-1.0.jar hbase/lib/

### Start HBase

    ./bin/start-hbase

You can check HBase is running fine by browsing to
[http://localhost:60010/](http://localhost:60010/)

## Demo

Start by creating a schema:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.DemoSchema

Important in the code of DemoSchema is that we enable replication by calling setScope(1)!


Next, start the logging consumer:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.LoggingConsumer


Upload some content:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.DemoIngester

(this continues forever, interrupt with ctrl+c)

To follow what's going on, you can use the hbase shell to insert an individual row:

    put 'sep-user-demo', 'my-user-id', 'info:name', 'Jules'

As you update data in HBase, the LoggingConsumer logs incoming update events.

Unfortunately, the put command in the shell only allows to set one column value at a time.

Now we can update this user to also add the email address:

    put 'sep-user-demo', 'my-user-id', 'info:email', 'jules@hotmail.com'


### Multiple subscribers

It is possible to register multiple event subscribers, each subscriber will receive all events.
The events will be distributed amongst the consumers started for that subscriber.

