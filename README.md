HBase Indexer
=============

HBase Indexer allows you to easily and quickly index HBase rows into Solr.
Usage documentation can be found on the hbase-indexer Wiki -
http://github.com/NGDATA/hbase-indexer/wiki.

## Subprojects

### HBase SEP

A standalone library for asynchronously processing HBase mutation events
by hooking into HBase replication, see [the SEP readme](hbase-sep/README.md).

### HBase SEP & replication monitoring

A standalone utility to monitor HBase replication progress,
see [the SEP-tools readme](hbase-sep/hbase-sep-tools/README.md).


## Building

You can build the full hbase-indexer project as follows:

    mvn clean install -DskipTests
