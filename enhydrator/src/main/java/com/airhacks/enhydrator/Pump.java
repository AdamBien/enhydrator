package com.airhacks.enhydrator;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.airhacks.enhydrator.flexpipe.EntryTransformation;
import com.airhacks.enhydrator.flexpipe.Pipeline;
import com.airhacks.enhydrator.in.ResultSetToEntries;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.out.Sink;
import com.airhacks.enhydrator.transform.EntryTransformer;
import com.airhacks.enhydrator.transform.Expression;
import com.airhacks.enhydrator.transform.FilterExpression;
import com.airhacks.enhydrator.transform.FunctionScriptLoader;
import com.airhacks.enhydrator.transform.RowTransformer;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.json.JsonValue;

/**
 *
 * @author airhacks.com
 */
public class Pump {

    private final Source source;
    private final Map<String, Function<Object, Object>> namedEntryFunctions;
    private final List<Function<Row, Row>> beforeTransformations;
    private final List<Function<Row, Row>> afterTransformations;
    private final List<String> expressions;
    private final List<String> filterExpressions;
    private final List<Sink> sinks;
    private final String sql;
    private final Object[] params;
    private final Expression expression;
    private final FilterExpression filterExpression;

    private final Sink deadLetterQueue;
    private final Consumer<String> flowListener;
    private long rowCount;

    private Pump(Source source, Function<ResultSet, Row> rowTransformer,
            List<Function<Row, Row>> before,
            Map<String, Function<Object, Object>> namedFunctions,
            List<String> filterExpressions,
            List<String> expressions,
            List<Function<Row, Row>> after,
            List<Sink> sinks,
            Sink dlq,
            String sql,
            Consumer<String> flowListener, Object... params) {
        this.flowListener = flowListener;
        this.filterExpressions = filterExpressions;
        this.expression = new Expression(flowListener);
        this.filterExpression = new FilterExpression(flowListener);
        this.source = source;
        this.beforeTransformations = before;
        this.namedEntryFunctions = namedFunctions;
        this.expressions = expressions;
        this.afterTransformations = after;
        this.sinks = sinks;
        this.deadLetterQueue = dlq;
        this.sql = sql;
        this.params = params;

    }

    public long start() {
        this.rowCount = 0;
        Iterable<Row> input = this.source.query(sql, params);
        this.flowListener.accept("Query executed: " + sql);
        this.sinks.forEach(s -> s.init());
        this.flowListener.accept("Sink initialized");
        input.forEach(this::onNewRow);
        this.flowListener.accept("Results processed");
        this.sinks.forEach(s -> s.close());
        this.flowListener.accept("Sink closed");
        return this.rowCount;

    }

    void onNewRow(Row row) {
        this.flowListener.accept("Processing: " + row.getNumberOfColumns() + " columns !");
        this.rowCount++;
        Optional<Boolean> first = this.filterExpressions.stream().
                map(e -> this.filterExpression.execute(row, e)).
                filter(r -> r == false).findFirst();
        if (!first.isPresent()) {
            transformRow(row);
        } else {
            this.flowListener.accept("Row ignored by filtering");
        }

    }

    void transformRow(Row convertedRow) {
        Row entryColumns = applyRowTransformations(this.beforeTransformations, convertedRow);
        applyNamedFunctions(entryColumns);
        this.flowListener.accept("Named functions processed");
        applyExpressions(convertedRow);
        this.flowListener.accept("Expressions processed");
        Row afterProcessed = applyRowTransformations(this.afterTransformations, entryColumns);
        if (afterProcessed == null) {
            return;
        }
        this.flowListener.accept("After process RowTransformer executed. " + afterProcessed.getNumberOfColumns() + " entries");
        this.sink(afterProcessed);
        this.flowListener.accept("Result processed by sinks");
    }

    void sink(Row afterProcessed) {
        this.flowListener.accept("Sinking " + afterProcessed.getNumberOfColumns() + " entries: " + afterProcessed);

        Map<String, Row> groupedByDestinations = afterProcessed.getColumnsGroupedByDestination();
        if (groupedByDestinations != null && !groupedByDestinations.isEmpty()) {
            this.sinks.forEach(s -> sink(s, groupedByDestinations));
        } else {
            this.flowListener.accept("Empty grouping received for sinks: " + this.sinks);
        }
    }

    void sink(Sink sink, Map<String, Row> groupByDestinations) {
        String destination = sink.getName();
        if (destination == null) {
            this.flowListener.accept(sink + " has a null destination, skipping");
            return;
        }
        Row entriesForSink = groupByDestinations.get(destination);
        if (entriesForSink != null) {
            this.flowListener.accept("Processing entries " + entriesForSink + " with " + destination);
            sink.processRow(entriesForSink);
            this.flowListener.accept("Entries processed!");
        } else {
            this.flowListener.accept("No entries found for: " + destination);
        }
    }

    void applyExpressions(Row current) {
        this.expressions.forEach(s -> applyExpression(current, s));

    }

    void applyExpression(Row current, String expression) {
        this.flowListener.accept("Executing expression: " + expression);
        try {
            this.expression.
                    execute(current, expression);
        } finally {
            this.flowListener.accept("Expression executed.");
        }
    }

    Object applyOrReturnOnNamed(String name, JsonValue value) {
        final Function<Object, Object> function = this.namedEntryFunctions.get(name);
        if (function != null) {
            this.flowListener.accept("Function: " + function + " found for name: " + name);
            return function.apply(value);
        } else {
            this.flowListener.accept("No function found for name: " + name);
            return value;
        }
    }

    void applyNamedFunctions(Row entryColumns) {
        this.namedEntryFunctions.forEach((k, v) -> entryColumns.transformColumn(k, v));
    }

    static Row applyRowTransformations(List<Function<Row, Row>> trafos, Row convertedColumns) {
        if (trafos == null || trafos.isEmpty()) {
            return convertedColumns;
        }
        final Function<Row, Row> composition = trafos.stream().reduce((i, j) -> i.andThen(j)).get();
        Row result = composition.apply(convertedColumns);
        if (result == null) {
            return null;
        } else {
            return result;
        }
    }

    public static class Engine {

        private List<Sink> sinks;
        private Sink deadLetterQueue;
        private Source source;
        private Function<ResultSet, Row> resultSetToEntries;
        private Map<String, Function<Object, Object>> entryFunctions;
        private Map<Integer, Function<Row, Row>> indexedFunctions;
        private List<Function<Row, Row>> before;
        private List<Function<Row, Row>> after;
        private FunctionScriptLoader loader;
        private List<String> expressions;
        private List<String> filterExpressions;
        private String sql;
        private Object[] params;
        private Consumer<String> flowListener;

        public Engine() {
            this.sinks = new ArrayList<>();
            this.expressions = new ArrayList<>();
            this.filterExpressions = new ArrayList<>();
            this.resultSetToEntries = new ResultSetToEntries();
            this.entryFunctions = new HashMap<>();
            this.before = new ArrayList<>();
            this.after = new ArrayList<>();
            this.indexedFunctions = new HashMap<>();
            this.loader = new FunctionScriptLoader();
            this.flowListener = f -> {
            };
            this.deadLetterQueue = new LogSink();
        }

        public Engine homeScriptFolder(String baseFolder) {
            this.loader = new FunctionScriptLoader(baseFolder);
            return this;
        }

        public Engine from(Source source) {
            this.source = source;
            return this;
        }

        public Engine to(Sink sink) {
            this.sinks.add(sink);
            return this;
        }

        public Engine dlq(Sink sink) {
            this.deadLetterQueue = sink;
            return this;
        }

        public Engine startWith(Function<Row, Row> before) {
            this.before.add(before);
            return this;
        }

        public Engine startWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            return startWith(rowTransformer::execute);
        }

        public Engine with(String entryName, Function<Object, Object> entryFunction) {
            this.entryFunctions.put(entryName, entryFunction);
            return this;
        }

        public Engine with(String columnName, String scriptName) {
            Function<Object, Object> function = load(scriptName);
            return with(columnName, function);
        }

        Function<Object, Object> load(String scriptName) {
            EntryTransformer entryTransformer = this.loader.getEntryTransformer(scriptName);
            return entryTransformer::execute;
        }

        public Engine endWith(Function<Row, Row> after) {
            this.after.add(after);
            return this;
        }

        public Engine endWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            return endWith(rowTransformer::execute);
        }

        public Engine sqlQuery(String sql, Object... params) {
            this.sql = sql;
            this.params = params;
            return this;
        }

        public Engine flowListener(Consumer<String> listener) {
            this.flowListener = listener;
            return this;
        }

        public Engine filter(String expression) {
            this.filterExpressions.add(expression);
            return this;
        }

        public Pump build() {
            return new Pump(source, this.resultSetToEntries,
                    this.before, this.entryFunctions,
                    this.filterExpressions,
                    this.expressions,
                    this.after, this.sinks,
                    this.deadLetterQueue,
                    this.sql,
                    this.flowListener,
                    this.params);
        }

        public Engine use(Pipeline pipeline) {
            homeScriptFolder(pipeline.getScriptsHome());
            this.source = pipeline.getSource();
            this.sinks = pipeline.getSinks();
            this.resultSetToEntries = new ResultSetToEntries();
            pipeline.getPreRowTransformers().forEach(t -> startWith(t));
            List<EntryTransformation> trafos = pipeline.getEntryTransformations();
            trafos.forEach(t -> {
                String name = t.getColumnName();
                if (name != null) {
                    with(name, t.getFunction());
                }
            });
            pipeline.getPostRowTransfomers().forEach(t -> endWith(t));
            this.expressions = pipeline.getExpressions();
            List<Object> queryParams = pipeline.getQueryParams();
            if (queryParams == null || queryParams.isEmpty()) {
                sqlQuery(pipeline.getSqlQuery());
            } else {
                sqlQuery(pipeline.getSqlQuery(), queryParams.toArray());
            }
            return this;
        }
    }
}
