package com.airhacks.enhydrator;

import com.airhacks.enhydrator.flexpipe.EntryTransformation;
import com.airhacks.enhydrator.flexpipe.JDBCPipeline;
import com.airhacks.enhydrator.flexpipe.Pipeline;
import com.airhacks.enhydrator.flexpipe.Plumber;
import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import com.airhacks.enhydrator.transform.EntryTransformer;
import com.airhacks.enhydrator.transform.Expression;
import com.airhacks.enhydrator.transform.FunctionScriptLoader;
import com.airhacks.enhydrator.transform.ResultSetToEntries;
import com.airhacks.enhydrator.transform.RowTransformer;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Pump {

    private final JDBCSource source;
    private final Function<ResultSet, List<Entry>> rowTransformer;
    private final Map<String, Function<Entry, List<Entry>>> namedEntryFunctions;
    private final Map<Integer, Function<Entry, List<Entry>>> indexedEntryFunctions;
    private final Function<List<Entry>, List<Entry>> before;
    private final Function<List<Entry>, List<Entry>> after;
    private final List<String> expressions;
    private final Sink sink;
    private final String sql;
    private final Object[] params;
    private final Expression expression;

    private Pump(JDBCSource source, Function<ResultSet, List<Entry>> rowTransformer,
            Function<List<Entry>, List<Entry>> before,
            Map<String, Function<Entry, List<Entry>>> namedFunctions,
            Map<Integer, Function<Entry, List<Entry>>> indexedFunctions,
            List<String> expressions,
            Function<List<Entry>, List<Entry>> after,
            Sink sink, String sql, Object... params) {
        this.expression = new Expression();
        this.source = source;
        this.before = before;
        this.rowTransformer = rowTransformer;
        this.namedEntryFunctions = namedFunctions;
        this.indexedEntryFunctions = indexedFunctions;
        this.expressions = expressions;
        this.after = after;
        this.sink = sink;
        this.sql = sql;
        this.params = params;
    }

    public void start() {
        Iterable<ResultSet> results = this.source.query(sql, params);
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
                map(e -> applyExpression(convertedColumns, e)).
                flatMap(l -> l.stream()).
                collect(Collectors.toList());
        List<Entry> afterProcessed = this.after.apply(transformed);
        this.sink.processRow(afterProcessed);

    }

    List<Entry> applyExpression(List<Entry> columns, Entry current) {
        return this.expressions.stream().
                map(e -> this.expression.
                        execute(columns, current, e)).
                flatMap(l -> l.stream()).
                collect(Collectors.toList());

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
        final Function<Entry, List<Entry>> function = this.namedEntryFunctions.get(e.getName());
        if (function != null) {
            return function.apply(e);
        } else {
            return e.asList();
        }
    }

    public static class Engine {

        private Sink sink;
        private JDBCSource source;
        private Function<ResultSet, List<Entry>> resultSetToEntries;
        private Map<String, Function<Entry, List<Entry>>> entryFunctions;
        private Map<Integer, Function<Entry, List<Entry>>> indexedFunctions;
        private Function<List<Entry>, List<Entry>> before;
        private Function<List<Entry>, List<Entry>> after;
        private FunctionScriptLoader loader;
        private List<String> expressions;
        private String sql;
        private Object[] params;

        public Engine() {
            this.expressions = new ArrayList<>();
            this.resultSetToEntries = new ResultSetToEntries();
            this.entryFunctions = new HashMap<>();
            this.before = f -> f;
            this.after = f -> f;
            this.indexedFunctions = new HashMap<>();
            this.loader = new FunctionScriptLoader();
        }

        public Engine homeScriptFolder(String baseFolder) {
            this.loader = new FunctionScriptLoader(baseFolder);
            return this;
        }

        public Engine from(JDBCSource source) {
            this.source = source;
            return this;
        }

        public Engine to(Sink sink) {
            this.sink = sink;
            return this;
        }

        public Engine startWith(Function<List<Entry>, List<Entry>> before) {
            this.before = before;
            return this;
        }

        public Engine startWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            return startWith(rowTransformer::execute);
        }

        public Engine with(String entryName, Function<Entry, List<Entry>> entryFunction) {
            this.entryFunctions.put(entryName, entryFunction);
            return this;
        }

        public Engine with(int index, Function<Entry, List<Entry>> entryFunction) {
            this.indexedFunctions.put(index, entryFunction);
            return this;
        }

        public Engine with(String entryName, String scriptName) {
            Function<Entry, List<Entry>> function = load(scriptName);
            return with(entryName, function);
        }

        public Engine with(int index, String scriptName) {
            Function<Entry, List<Entry>> function = load(scriptName);
            return with(index, function);
        }

        Function<Entry, List<Entry>> load(String scriptName) {
            EntryTransformer entryTransformer = this.loader.getEntryTransformer(scriptName);
            return entryTransformer::execute;
        }

        public Engine endWith(Function<List<Entry>, List<Entry>> after) {
            this.after = after;
            return this;
        }

        public Engine endWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            return endWith(rowTransformer::execute);
        }

        public Engine sql(String sql, Object... params) {
            this.sql = sql;
            this.params = params;
            return this;
        }

        public Pump build() {
            return new Pump(source, this.resultSetToEntries,
                    this.before, this.entryFunctions, this.indexedFunctions,
                    this.expressions, this.after, this.sink, this.sql, this.params);
        }

        public Pump use(Pipeline pipeline) {
            this.source = pipeline.getSource();
            this.resultSetToEntries = new ResultSetToEntries();
            pipeline.getPreRowTransformers().forEach(t -> startWith(t));
            List<EntryTransformation> trafos = pipeline.getEntryTransformations();
            trafos.forEach(t -> {
                String name = t.getColumnName();
                if (name != null) {
                    with(name, t.getFunction());
                } else {
                    Integer slot = t.getSlot();
                    Objects.requireNonNull(slot, "Column name was null, slot has to be set");
                    with(slot, t.getFunction());
                }
            });
            pipeline.getPostRowTransfomers().forEach(t -> endWith(t));
            this.expressions = pipeline.getExpressions();
            List<Object> queryParams = pipeline.getQueryParams();
            if (queryParams == null || queryParams.isEmpty()) {
                sql(pipeline.getSqlQuery());
            } else {
                sql(pipeline.getSqlQuery(), queryParams.toArray());
            }
            return build();
        }
    }
}
