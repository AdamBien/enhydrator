package com.airhacks.enhydrator;

import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.out.SystemOutSink;
import com.airhacks.enhydrator.transform.ResultSetToEntries;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Driver {

    private final Source source;
    private final Function<ResultSet, List<Entry>> rowTransformer;
    private final Map<String, Function<Entry, List<Entry>>> entryFunctions;
    private final Consumer<List<Entry>> sink;

    private Driver(Source source, Function<ResultSet, List<Entry>> rowTransformer, Map<String, Function<Entry, List<Entry>>> entryFunctions, Consumer<List<Entry>> sink) {
        this.source = source;
        this.rowTransformer = rowTransformer;
        this.entryFunctions = entryFunctions;
        this.sink = sink;
    }

    void process(String sql) {
        Iterable<ResultSet> results = this.source.query(sql);
        results.forEach(this::onNewRow);
    }

    void onNewRow(ResultSet columns) {
        List<Entry> entryColumns = this.rowTransformer.apply(columns);

        List<Entry> transformedColumns = entryColumns.stream().
                filter(e -> entryFunctions.containsKey(e.getName())).
                map(e -> entryFunctions.get(e.getName()).apply(e)).
                flatMap(l -> l.stream()).
                collect(Collectors.toList());

        this.sink.accept(transformedColumns);

    }

    public static class Drive {

        private Consumer<List<Entry>> sink;
        private Source source;
        private Function<ResultSet, List<Entry>> resultSetToEntries;
        private Map<String, Function<Entry, List<Entry>>> entryFunctions;

        public Drive() {
            this.resultSetToEntries = new ResultSetToEntries();
            this.entryFunctions = new HashMap<>();
        }

        public Drive from(Source source) {
            this.source = source;
            return this;
        }

        public Drive to(Consumer<List<Entry>> sink) {
            this.sink = sink;
            return this;
        }

        public Drive with(String entryName, Function<Entry, List<Entry>> entryFunction) {
            this.entryFunctions.put(entryName, entryFunction);
            return this;
        }

        public void go(String sql) {
            Driver driver = new Driver(this.source, this.resultSetToEntries, this.entryFunctions, this.sink);
            driver.process(sql);
        }

    }

}
