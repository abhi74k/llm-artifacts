# Complete Guide to Apache Parquet with Java

## Table of Contents
- [Introduction](#introduction)
- [Setup](#setup)
- [Basic Concepts](#basic-concepts)
- [Writing Parquet Files](#writing-parquet-files)
  - [Simple Types](#simple-types)
  - [Nested Structures](#nested-structures)
  - [Arrays and Lists](#arrays-and-lists)
- [Reading Parquet Files](#reading-parquet-files)
  - [Basic Reading](#basic-reading)
  - [Selective Column Reading](#selective-column-reading)
  - [Filtering](#filtering)
- [Best Practices](#best-practices)
- [Common Issues](#common-issues)

## Introduction

Apache Parquet is a columnar storage format that provides efficient compression and encoding schemes. It's particularly useful for:
- Big Data processing
- Analytics workloads
- Data warehousing
- Systems requiring efficient storage and retrieval

Key benefits:
- Efficient compression
- Column-based storage (read only what you need)
- Schema evolution support
- Widely supported across the Big Data ecosystem

## Setup

Add these dependencies to your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-common</artifactId>
        <version>1.13.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-hadoop</artifactId>
        <version>1.13.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-common</artifactId>
        <version>3.3.6</version>
    </dependency>
</dependencies>
```

## Basic Concepts

### Schema Definition
Parquet uses a schema to define the structure of your data. Here's a simple example:

```
message Employee {
  required int64 id;
  required binary name (UTF8);
  optional int32 age;
  optional binary email (UTF8);
}
```

Schema types:
- `required`: Field must have a value
- `optional`: Field can be null
- Common types: int32, int64, boolean, binary, float, double

## Writing Parquet Files

### Simple Types

Here's a basic example writing primitive types:

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class SimpleParquetWriter {
    public static void main(String[] args) throws Exception {
        // Define schema
        String schema = "message Employee {\n" +
                       "  required int64 id;\n" +
                       "  required binary name (UTF8);\n" +
                       "  optional int32 age;\n" +
                       "  optional binary email (UTF8);\n" +
                       "}";
        
        MessageType messageType = MessageTypeParser.parseMessageType(schema);
        Path path = new Path("employee.parquet");
        Configuration conf = new Configuration();
        
        // Set schema into configuration
        GroupWriteSupport.setSchema(messageType, conf);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        
        // Create writer
        try (ParquetWriter<Group> writer = ParquetWriter.builder(path)
                .withConf(conf)
                .withWriteSupport(writeSupport)
                .build()) {
            
            // Write some records
            SimpleGroup group = new SimpleGroup(messageType);
            group.add("id", 1L);
            group.add("name", "John Doe");
            group.add("age", 30);
            group.add("email", "john@example.com");
            
            writer.write(group);
        }
    }
}
```

### Nested Structures

For nested structures, you define a more complex schema:

```java
public class NestedParquetWriter {
    public static void main(String[] args) throws Exception {
        // Define schema with nested structure
        String schema = "message Employee {\n" +
                       "  required int64 id;\n" +
                       "  required binary name (UTF8);\n" +
                       "  optional group address {\n" +
                       "    optional binary street (UTF8);\n" +
                       "    optional binary city (UTF8);\n" +
                       "    optional binary country (UTF8);\n" +
                       "  }\n" +
                       "}";
        
        MessageType messageType = MessageTypeParser.parseMessageType(schema);
        Path path = new Path("employee_nested.parquet");
        Configuration conf = new Configuration();
        
        GroupWriteSupport.setSchema(messageType, conf);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        
        try (ParquetWriter<Group> writer = ParquetWriter.builder(path)
                .withConf(conf)
                .withWriteSupport(writeSupport)
                .build()) {
            
            SimpleGroup group = new SimpleGroup(messageType);
            group.add("id", 1L);
            group.add("name", "John Doe");
            
            // Add nested group
            Group address = group.addGroup("address");
            address.add("street", "123 Main St");
            address.add("city", "New York");
            address.add("country", "USA");
            
            writer.write(group);
        }
    }
}
```

### Arrays and Lists

Here's how to work with arrays/repeated fields:

```java
public class ArrayParquetWriter {
    public static void main(String[] args) throws Exception {
        // Define schema with repeated fields
        String schema = "message Employee {\n" +
                       "  required int64 id;\n" +
                       "  required binary name (UTF8);\n" +
                       "  repeated binary phoneNumbers (UTF8);\n" +
                       "  repeated group previousRoles {\n" +
                       "    required binary title (UTF8);\n" +
                       "    required int32 year;\n" +
                       "  }\n" +
                       "}";
        
        MessageType messageType = MessageTypeParser.parseMessageType(schema);
        Path path = new Path("employee_arrays.parquet");
        Configuration conf = new Configuration();
        
        GroupWriteSupport.setSchema(messageType, conf);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        
        try (ParquetWriter<Group> writer = ParquetWriter.builder(path)
                .withConf(conf)
                .withWriteSupport(writeSupport)
                .build()) {
            
            SimpleGroup group = new SimpleGroup(messageType);
            group.add("id", 1L);
            group.add("name", "John Doe");
            
            // Add repeated primitive field
            group.add("phoneNumbers", "+1234567890");
            group.add("phoneNumbers", "+9876543210");
            
            // Add repeated group
            Group role1 = group.addGroup("previousRoles");
            role1.add("title", "Developer");
            role1.add("year", 2020);
            
            Group role2 = group.addGroup("previousRoles");
            role2.add("title", "Senior Developer");
            role2.add("year", 2022);
            
            writer.write(group);
        }
    }
}
```

## Reading Parquet Files

### Basic Reading

Here's a simple reader:

```java
public class SimpleParquetReader {
    public static void main(String[] args) throws Exception {
        Path path = new Path("employee.parquet");
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile
            .fromPath(path, new Configuration()));
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        PageReadStore pages;
        
        while ((pages = reader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO
                .getRecordReader(pages, new GroupRecordConverter(schema));
            
            for (int i = 0; i < rows; i++) {
                Group group = recordReader.read();
                System.out.println("Id: " + group.getLong("id", 0));
                System.out.println("Name: " + group.getString("name", 0));
                
                if (group.getFieldRepetitionCount("age") > 0) {
                    System.out.println("Age: " + group.getInteger("age", 0));
                }
            }
        }
        reader.close();
    }
}
```

### Selective Column Reading

Reading only specific columns:

```java
public class SelectiveParquetReader {
    public static void readSelectiveColumns(String filePath, String[] columns) 
            throws IOException {
        Path path = new Path(filePath);
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile
            .fromPath(path, new Configuration()));
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        PageReadStore pages;
        
        while ((pages = reader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO
                .getRecordReader(pages, new GroupRecordConverter(schema));
            
            long rows = pages.getRowCount();
            for (int i = 0; i < rows; i++) {
                Group group = recordReader.read();
                for (String column : columns) {
                    if (group.getFieldRepetitionCount(column) > 0) {
                        System.out.println(column + ": " + 
                            group.getValueToString(column, 0));
                    }
                }
            }
        }
        reader.close();
    }
}
```

### Reading Nested Structures

```java
public class NestedParquetReader {
    public static void readNested(String filePath) throws IOException {
        Path path = new Path(filePath);
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile
            .fromPath(path, new Configuration()));
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        PageReadStore pages;
        
        while ((pages = reader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO
                .getRecordReader(pages, new GroupRecordConverter(schema));
            
            long rows = pages.getRowCount();
            for (int i = 0; i < rows; i++) {
                Group group = recordReader.read();
                
                System.out.println("Employee ID: " + group.getLong("id", 0));
                System.out.println("Name: " + group.getString("name", 0));
                
                // Read nested address group
                if (group.getFieldRepetitionCount("address") > 0) {
                    Group address = group.getGroup("address", 0);
                    System.out.println("Street: " + 
                        address.getString("street", 0));
                    System.out.println("City: " + 
                        address.getString("city", 0));
                    System.out.println("Country: " + 
                        address.getString("country", 0));
                }
                
                // Read repeated fields (arrays)
                if (group.getFieldRepetitionCount("phoneNumbers") > 0) {
                    int phones = group.getFieldRepetitionCount("phoneNumbers");
                    System.out.println("Phone numbers:");
                    for (int j = 0; j < phones; j++) {
                        System.out.println("  " + 
                            group.getString("phoneNumbers", j));
                    }
                }
            }
        }
        reader.close();
    }
}
```

## Best Practices

1. **Schema Design**
   - Keep schema evolution in mind
   - Use optional fields when possible
   - Consider column ordering (most used first)

2. **Performance**
   - Use appropriate compression (Snappy for balance, GZIP for best compression)
   - Set reasonable row group sizes
   - Use dictionary encoding for low-cardinality fields

```java
ParquetWriter<Group> writer = ParquetWriter.builder(path)
    .withConf(conf)
    .withWriteSupport(writeSupport)
    .withCompressionCodec(CompressionCodecName.SNAPPY)
    .withDictionaryEncoding(true)
    .withValidation(true)
    .withRowGroupSize(128 * 1024 * 1024)  // 128MB row groups
    .build();
```

3. **Resource Management**
   - Always close readers and writers
   - Use try-with-resources
   - Handle exceptions properly

## Common Issues

1. **OutOfMemoryError**
   - Use row group size limits
   - Read in chunks
   - Don't load entire file into memory

2. **Schema Evolution**
   - Add only optional fields
   - Don't remove required fields
   - Don't change field types

3. **Performance Issues**
   - Use column projection
   - Apply predicate pushdown
   - Monitor memory usage

## Additional Resources

- [Apache Parquet Documentation](https://parquet.apache.org/documentation/latest/)
- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Hadoop Documentation](https://hadoop.apache.org/docs/current/)