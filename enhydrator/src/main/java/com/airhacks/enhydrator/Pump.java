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
import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.ResultSetToEntries;
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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author airhacks.com
 */
public class Pump {

    private final Source source;
    private final Map<String, Function<Entry, List<Entry>>> namedEntryFunctions;
    private final Map<Integer, Function<Entry, List<Entry>>> indexedEntryFunctions;
    private final List<Function<List<Entry>, List<Entry>>> beforeTransformations;
    private final List<Function<List<Entry>, List<Entry>>> afterTransformations;
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

    private Pump(Source source, Function<ResultSet, List<Entry>> rowTransformer,
            List<Function<List<Entry>, List<Entry>>> before,
            Map<String, Function<Entry, List<Entry>>> namedFunctions,
            Map<Integer, Function<Entry, List<Entry>>> indexedFunctions,
            List<String> filterExpressions,
            List<String> expressions,
            List<Function<List<Entry>, List<Entry>>> after,
            List<Sink> sink,
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
        this.indexedEntryFunctions = indexedFunctions;
        this.expressions = expressions;
        this.afterTransformations = after;
        this.sinks = sink;
        this.deadLetterQueue = dlq;
        this.sql = sql;
        this.params = params;

    }

    public long start() {
        this.rowCount = 0;
        Iterable<List<Entry>> input = this.source.query(sql, params);
        this.flowListener.accept("Query executed: " + sql);
        this.sinks.forEach(s -> s.init());
        this.flowListener.accept("Sink initialized");
        input.forEach(this::onNewRow);
        this.flowListener.accept("Results processed");
        this.sinks.forEach(s -> s.close());
        this.flowListener.accept("Sink closed");
        return this.rowCount;

    }

    void onNewRow(List<Entry> columns) {
        this.rowCount++;
        Optional<Boolean> first = this.filterExpressions.stream().
                map(e -> this.filterExpression.execute(columns, e)).
                filter(r -> r == false).findFirst();
        if (!first.isPresent()) {
            transformRow(columns);
        } else {
            this.flowListener.accept("Row ignored by filtering");
        }

    }

    void transformRow(List<Entry> convertedColumns) {
        List<Entry> entryColumns = applyRowTransformations(this.beforeTransformations, convertedColumns);
        final Stream<Entry> indexed = entryColumns.stream().
                map(e -> applyOrReturnOnIndexed(e)).
                flatMap(l -> l.stream());
        this.flowListener.accept("Indexed functions processed");
        final Stream<Entry> named = indexed.
                map(e -> applyOrReturnOnNamed(e)).
                flatMap(l -> l.stream());
        this.flowListener.accept("Named functions processed");
        final Stream<Entry> expressionList = named.
                map(e -> applyExpressions(convertedColumns, e)).
                flatMap(l -> l.stream());
        this.flowListener.accept("Expressions processed");
        List<Entry> transformed = expressionList.
                collect(Collectors.toList());
        this.flowListener.accept("Result collected");
        List<Entry> afterProcessed = applyRowTransformations(this.afterTransformations, transformed);
        this.flowListener.accept("After process RowTransformer executed.");
        this.sink(afterProcessed);
        this.flowListener.accept("Result passed to sinks");
    }

    void sink(List<Entry> afterProcessed) {
        Map<String, List<Entry>> groupedByDestinations = groupByDestinations(afterProcessed);
        if (groupedByDestinations != null && !groupedByDestinations.isEmpty()) {
            this.sinks.forEach(s -> sink(s, groupedByDestinations));
        } else {
            this.flowListener.accept("Empty grouping received for sinks: " + this.sinks);
        }
    }

    void sink(Sink sink, Map<String, List<Entry>> groupByDestinations) {
        String destination = sink.getName();
        if (destination == null) {
            this.flowListener.accept(sink + " has a null destination, skipping");
            return;
        }
        List<Entry> entriesForSink = groupByDestinations.get(destination);
        if (entriesForSink != null) {
            this.flowListener.accept("Processing entries " + entriesForSink + " with " + destination);
            sink.processRow(entriesForSink);
            this.flowListener.accept("Entries processed!");
        } else {
            this.flowListener.accept("No entries found for: " + destination);
        }
    }

    Map<String, List<Entry>> groupByDestinations(List<Entry> transformationResult) {
        return transformationResult.
                stream().
                collect(Collectors.groupingBy(e -> e.getDestination()));
    }

    List<Entry> applyExpressions(List<Entry> columns, Entry current) {
        if (this.expressions == null || this.expressions.isEmpty()) {
            return current.asList();
        }
        return this.expressions.stream().
                map(e -> applyExpression(columns, current, e)).
                flatMap(l -> l.stream()).
                collect(Collectors.toList());

    }

    List<Entry> applyExpression(List<Entry> columns, Entry current, String expression) {
        this.flowListener.accept("Executing expression: " + expression);
        try {
            return this.expression.
                    execute(columns, current, expression);
        } finally {
            this.flowListener.accept("Expression executed.");
        }
    }

    List<Entry> applyOrReturnOnIndexed(Entry e) {
        final int slot = e.getSlot();
        final Function<Entry, List<Entry>> function = this.indexedEntryFunctions.get(slot);
        if (function != null) {
            this.flowListener.accept("Function: " + function + " found for slot: " + slot);
            return function.apply(e);
        } else {
            this.flowListener.accept("No function found for slot: " + slot);
            return e.asList();
        }
    }

    List<Entry> applyOrReturnOnNamed(Entry e) {
        final String name = e.getName();
        final Function<Entry, List<Entry>> function = this.namedEntryFunctions.get(name);
        if (function != null) {
            this.flowListener.accept("Function: " + function + " found for name: " + name);
            return function.apply(e);
        } else {
            this.flowListener.accept("No function found for name: " + name);
            return e.asList();
        }
    }

    static List<Entry> applyRowTransformations(List<Function<List<Entry>, List<Entry>>> trafos, List<Entry> convertedColumns) {
        if (trafos == null || trafos.isEmpty()) {
            return convertedColumns;
        }
        return trafos.stream().
                map(r -> r.apply(convertedColumns)).
                filter(l -> (l != null && !l.isEmpty())).
                flatMap(l -> l.stream()).collect(Collectors.toList());
    }

    public static class Engine {

        private List<Sink> sinks;
        private Sink deadLetterQueue;
        private Source source;
        private Function<ResultSet, List<Entry>> resultSetToEntries;
        private Map<String, Function<Entry, List<Entry>>> entryFunctions;
        private Map<Integer, Function<Entry, List<Entry>>> indexedFunctions;
        private List<Function<List<Entry>, List<Entry>>> before;
        private List<Function<List<Entry>, List<Entry>>> after;
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

        public Engine startWith(Function<List<Entry>, List<Entry>> before) {
            this.before.add(before);
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
                    this.before, this.entryFunctions, this.indexedFunctions,
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
                sqlQuery(pipeline.getSqlQuery());
            } else {
                sqlQuery(pipeline.getSqlQuery(), queryParams.toArray());
            }
            return this;
        }
    }
}
