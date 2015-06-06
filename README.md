enhydrator
==========

Java 8 ETL toolkit without dependencies. Enhydrator reads table-like structures, filters, transforms and writes them back.

# How it works

Enhydrator reads the data from a `Source`, filters, transforms and writes it back to a `Sink`.

## Source

The source is responsible for converting an external information into an `Iterable` of `Row`s.

```java
@FunctionalInterface
public interface Source {

    Iterable<Row> query(String query, Object… params);

    default Iterable<Row> query() {
        return this.query(null);
    }

}
```
Enhydrator ships with `CSVFileSource`, `CSVStreamSource`, `JDBCSource`, `ScriptableSource` and `VirtualSinkSource` (a in-memory source and sink at the same time).

## Row

The essential data structure is `Row`. A row comprises `Column`s accessible by index and / or a name:

```java
public class Row {

    private final Map<String, Column> columnByName;
    private final Map<Integer, Column> columnByIndex;
//…
}
```

## Column

A Column holds an index, name and an optional value:

```java
public class Column implements Cloneable {

    private int index;
    private String name;
    private Optional<Object> value;
		//…
}
```

## Sink

Sink is the Source’s counterpart:

```java
public abstract class Sink implements AutoCloseable {

    public abstract void processRow(Row entries);

}
```

Each transformed `Row` is passed to the Sink. Enhydrator ships with `CSVFileSink`, `JDBCSink`, `LogSink`, `PojoSink` (a `Row` to Object mapper), `RowSink` and `VirtualSinkSource`.


