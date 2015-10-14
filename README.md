enhydrator
==========

Java 8 ETL toolkit without dependencies. Enhydrator reads table-like structures, filters, transforms and writes them back.

# Installation

```xml
        <dependency>
            <groupId>com.airhacks</groupId>
            <artifactId>enhydrator</artifactId>
            <version>[RECENT-VERSION]</version>
        </dependency>
```
# For Absolute Beginners

[![Hello Enhydrator](https://i.ytimg.com/vi/be9lK1KTLrQ/mqdefault.jpg)](https://www.youtube.com/embed/be9lK1KTLrQ?rel=0)

# Reading CSV Into Rows and Filtering

[![Reading CSV Into Rows and Filtering](https://i.ytimg.com/vi/O8D9hVRHjTQ/mqdefault.jpg)](https://www.youtube.com/embed/O8D9hVRHjTQ?rel=0)

# Mapping Rows To POJOs

[![Mapping Rows To PoJOs](https://i.ytimg.com/vi/L-jjYTm1xI8/mqdefault.jpg)](https://www.youtube.com/embed/L-jjYTm1xI8?rel=0)

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

## Filter expressions

Filter expression is a JavaScript (Nashorn) snippet evaluated against the current row. The script has to return a Boolean `true`. Anything else is going to be interpreted as `false` and will skip the processing of current row.

The current `Row` instance is passed to the script as a variable `$ROW`. In addition to the current Row, also `$MEMORY` (a map-like structure available for the entire processing pipeline), `$EMPTY` (an empty row) and also programmatically passed variables are accessible.

## Transformation

Each row is going to be transformed according to the following schema:

1. All configured filter expressions are evaluated against the current row and have to return `true`.
2. Pre-Row transformations are executed. A row transformation is a function: `Function<Row, Row>`. "Row in, Row out"
3. Row expressions are executed agains the current row with the same variables (`$ROW`,`$EMPTY` etc.) as filters. A row expression does not have to return anything (is `void`).
4. Column transformations are executed on the actual values: `Function<Object, Object>` of the `Column`.
5. Post-Row transformations are executed as in 2.
6. The remaining `Row` is passed to the Sink instance.

## Sample

The following `language.csv` file is filtered for Language "java" and the corresponding column "rank" is converted to an `Integer`

```
language;rank
java;1
c;2
cobol;3
esoteric;4
```
The following test should pass. See the origin test [FromJsonToCSVTest.java](https://github.com/AdamBien/enhydrator/blob/master/samples/json2csv/src/test/java/com/airhacks/samples/json/FromJsonToCSVTest.java):

```java
    @Test
    public void filterAndCastFromCSVFileToLog() {
        Source source = new CSVFileSource(INPUT + "/languages.csv", ";", "utf-8", true);
        VirtualSinkSource sink = new VirtualSinkSource();
        Pump pump = new Pump.Engine().
                from(source).
                filter("$ROW.getColumnValue('language') === 'java'").
                startWith(new DatatypeNameMapper().addMapping("rank", Datatype.INTEGER)).
                to(sink).
                to(new LogSink()).
                build();
        Memory memory = pump.start();
        assertFalse(memory.areErrorsOccured());
        assertThat(memory.getProcessedRowCount(), is(5l));

        //expecting only "java" language
        assertThat(sink.getNumberOfRows(), is(1));
        String languageValue = (String) sink.getRow(0).getColumnValue("language");
        assertThat(languageValue, is("java"));

        //expecting "java" having rank 1 as Integer
        Object rankValue = sink.getRow(0).getColumnValue("rank");
        assertTrue(rankValue instanceof Integer);
        assertThat((Integer) rankValue, is(1));
    }
```

