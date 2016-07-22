Phoenix HBase Indexer plugins
=============================

A set of plugin ByteArrayValueMappers to allow using hbase-indexer to index data stored
in [Apache Phoenix](http://phoenix.apache.org).

Usage involves supplying the include ByteArrayValueMapper implementation class names for the `type` attribute in a
`field` definition in an indexer definition. For example, to index the `COMPANY` field of the `STOCK_SYMBOL` in the
Phoenix samples, you would set up an hbase-indexer configuration as follows:

    <indexer table="STOCK_SYMBOL"
      <field name="COMPANY" 
             value="0:COMPANY" 
             type="com.ngdata.hbaseindexer.phoenix.VarcharMapper"/>
    </indexer>

