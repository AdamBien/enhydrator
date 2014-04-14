package com.airhacks.enhydrator;

import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import com.airhacks.enhydrator.transform.EntryTransformer;
import com.airhacks.enhydrator.transform.FunctionScriptLoader;
import com.airhacks.enhydrator.transform.ResultSetToEntries;
import com.airhacks.enhydrator.transform.RowTransformer;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Driver {

    private final JDBCSource source;
    private final Function<ResultSet, List<Entry>> rowTransformer;
    private final Map<String, Function<Entry, List<Entry>>> namedEntryFunctions;
    private final Map<Integer, Function<Entry, List<Entry>>> indexedEntryFunctions;
    private final Function<List<Entry>, List<Entry>> before;
    private final Function<List<Entry>, List<Entry>> after;

    private final Sink sink;

    private Driver(JDBCSource source, Function<ResultSet, List<Entry>> rowTransformer,
            Function<List<Entry>, List<Entry>> before,
            Map<String, Function<Entry, List<Entry>>> namedFunctions,
            Map<Integer, Function<Entry, List<Entry>>> indexedFunctions,
            Function<List<Entry>, List<Entry>> after,
            Sink sink) {
        this.source = source;
        this.before = before;
        this.rowTransformer = rowTransformer;
        this.namedEntryFunctions = namedFunctions;
        this.indexedEntryFunctions = indexedFunctions;
        this.after = after;
        this.sink = sink;
    }

    void process(String sql) {
        Iterable<ResultSet> results = this.source.query(sql);
        this.sink.init();
        results.forEach(this::onNewRow);
        this.sink.close();
    }

    void onNewRow(ResultSet columns) {
        List<Entry> convertedColumns = this.rowTransformer.apply(columns);
        List<Entry> entryColumns = this.before.apply(convertedColumns);
        List<Entry> transformed = entryColumns.stream().
                map(e -> applyOrReturnOnIndexed(e)).
                flatMap(l -> l.stream()).
                map(e -> applyOrReturnOnNamed(e)).
                flatMap(l -> l.stream()).
                collect(Collectors.toList());
        List<Entry> afterProcessed = this.after.apply(transformed);
        this.sink.processRow(afterProcessed);

    }

    List<Entry> applyOrReturnOnIndexed(Entry e) {
        final Function<Entry, List<Entry>> function = this.indexedEntryFunctions.get(e.getSlot());
        if (function != null) {
            return function.apply(e);
        } else {
            return e.asList();
        }
    }

    List<Entry> applyOrReturnOnNamed(Entry e) {
        final Function<Entry, List<Entry>> function = this.namedEntryFunctions.get(e.getSlot());
        if (function != null) {
            return function.apply(e);
        } else {
            return e.asList();
        }
    }

    public static class Drive {

        private Sink sink;
        private JDBCSource source;
        private Function<ResultSet, List<Entry>> resultSetToEntries;
        private Map<String, Function<Entry, List<Entry>>> entryFunctions;
        private Map<Integer, Function<Entry, List<Entry>>> indexedFunctions;
        private Function<List<Entry>, List<Entry>> before;
        private Function<List<Entry>, List<Entry>> after;
        private FunctionScriptLoader loader;

        public Drive() {
            this.resultSetToEntries = new ResultSetToEntries();
            this.entryFunctions = new HashMap<>();
            this.before = f -> f;
            this.after = f -> f;
            this.indexedFunctions = new HashMap<>();
            this.loader = new FunctionScriptLoader();
        }

        public Drive homeScriptFolder(String baseFolder) {
            this.loader = new FunctionScriptLoader(baseFolder);
            return this;
        }

        public Drive from(JDBCSource source) {
            this.source = source;
            return this;
        }

        public Drive to(Sink sink) {
            this.sink = sink;
            return this;
        }

        public Drive startWith(Function<List<Entry>, List<Entry>> before) {
            this.before = before;
            return this;
        }

        public Drive startWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            return startWith(rowTransformer::execute);
        }

        public Drive with(String entryName, Function<Entry, List<Entry>> entryFunction) {
            this.entryFunctions.put(entryName, entryFunction);
            return this;
        }

        public Drive with(int index, Function<Entry, List<Entry>> entryFunction) {
            this.indexedFunctions.put(index, entryFunction);
            return this;
        }

        public Drive with(String entryName, String scriptName) {
            Function<Entry, List<Entry>> function = load(scriptName);
            return with(entryName, function);
        }

        public Drive with(int index, String scriptName) {
            Function<Entry, List<Entry>> function = load(scriptName);
            return with(index, function);
        }

        Function<Entry, List<Entry>> load(String scriptName) {
            EntryTransformer entryTransformer = this.loader.getEntryTransformer(scriptName);
            return entryTransformer::execute;
        }

        public Drive endWith(Function<List<Entry>, List<Entry>> after) {
            this.after = after;
            return this;
        }

        public Drive endWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            return endWith(rowTransformer::execute);
        }

        public void go(String sql) {
            Driver driver = new Driver(this.source, this.resultSetToEntries,
                    this.before, this.entryFunctions, this.indexedFunctions,
                    this.after, this.sink);
            driver.process(sql);
        }
    }
}
