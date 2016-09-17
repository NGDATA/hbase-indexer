# HBase Indexer Phoenix

This module contains command for transformations from Phoenix data types in bytes to Solr documents, based on Kite Morphlines.
To load Phoenix data in bytes use the `extractHBaseCells` command and then `convertPhoenixType` command to convert bytes to values.

## Example:
```
morphlines : [
  {
    id : morphline1
    importCommands : ["org.kitesdk.**", "com.ngdata.**"]
    
    commands : [
      {
        extractHBaseCells {
          mappings : [
            {
              inputColumn : "cfA:prefix*"
              outputField : fieldA
              type : "byte[]"
              source : qualifier
            },
            
            {
              inputColumn : "cfB:qual*"
              outputField : fieldB
              type : "byte[]"
              source: value
            }
          ]
        }
      },
      
      {
        convertPhoenixType {
          fields : [
            {
              # outputField from extractHBaseCells command
              fieldName : fieldA
              
              # PDataType enum name
              type : INTEGER
              
              # offset in bytes to handle prefixed or joined values, default is 0
              offset : 6
              
              # length in bytes must be set if offset is set 
              length: 4
              
              # SortOrder enum name, default is ASC
              sortOrder : ASC
            },
            
            {
              fieldName : fieldB
              type : LONG_ARRAY
              sortOrder : DESC
            }
          ]
        }
      }
      
      { logDebug { format : "output record: {}", args : ["@{}"] } }    
    ]
  }
]
```