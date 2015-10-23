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
import com.airhacks.enhydrator.flexpipe.ColumnTransformation;
import com.airhacks.enhydrator.flexpipe.Pipeline;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.out.SinkTemplate;
import com.airhacks.enhydrator.transform.ColumnTransformer;
import com.airhacks.enhydrator.transform.Expression;
import com.airhacks.enhydrator.transform.FilterExpression;
import com.airhacks.enhydrator.transform.FunctionScriptLoader;
import com.airhacks.enhydrator.transform.Memory;
import com.airhacks.enhydrator.transform.RowTransformer;
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
    private final Map<String, Function<Object, Object>> columnTransformations;
    private final List<Function<Row, Row>> beforeTransformations;
    private final List<Function<Row, Row>> afterTransformations;
    private final List<String> expressions;
    private final List<String> filterExpressions;
    private final List<SinkTemplate> sinks;
    private final String sql;
    private final Object[] params;
    private final Expression expression;
    private final FilterExpression filterExpression;

    private final SinkTemplate deadLetterQueue;
    private final Consumer<String> flowListener;
    private final Memory pumpMemory;
    private final boolean stopOnError;
    private final Map<String, Object> scriptEngineBindings;

    private Pump(Source source,
            List<Function<Row, Row>> before,
            Map<String, Function<Object, Object>> namedFunctions,
            List<String> filterExpressions,
            List<String> expressions,
            List<Function<Row, Row>> after,
            List<SinkTemplate> sinks,
            SinkTemplate dlq,
            String sql,
            Consumer<String> flowListener,
            boolean stopOnError,
            Memory pumpMemory,
            Map<String, Object> scriptEngineBindings,
            Object... params) {

        this.flowListener = flowListener;
        this.filterExpressions = filterExpressions;
        this.expression = new Expression(flowListener, scriptEngineBindings);
        this.filterExpression = new FilterExpression(flowListener, scriptEngineBindings);
        this.source = source;
        this.beforeTransformations = before;
        this.columnTransformations = namedFunctions;
        this.expressions = expressions;
        this.afterTransformations = after;
        this.sinks = sinks;
        this.deadLetterQueue = dlq;
        this.sql = sql;
        this.params = params;
        this.stopOnError = stopOnError;
        this.pumpMemory = pumpMemory;
        this.scriptEngineBindings = scriptEngineBindings;
    }

    public Memory start() {
        Iterable<Row> input = this.source.query(sql, params);
        this.flowListener.accept("Query executed: " + sql);
        this.sinks.forEach(s -> s.init());
        this.flowListener.accept("Sink initialized");
        if (this.stopOnError) {
            this.flowListener.accept("Erroneous rows will stop the pipeline");
            input.forEach(this::onNewRow);
        } else {
            this.flowListener.accept("Ignoring processing errors");
            input.forEach(this::processAndIgnoreErrors);
        }
        this.flowListener.accept("Results processed");
        this.sinks.forEach(s -> s.close());
        this.flowListener.accept("Sink closed");
        return this.pumpMemory;

    }

    void processAndIgnoreErrors(Row row) {
        try {
            onNewRow(row);
        } catch (Throwable ex) {
            row.errorOccured(ex);
        }
    }

    void onNewRow(Row row) {
        row.useMemory(pumpMemory);
        this.flowListener.accept("Processing: " + row.getNumberOfColumns() + " columns !");
        Optional<Boolean> first = this.filterExpressions.stream().
                map(e -> this.filterExpression.execute(row, e)).
                filter(r -> r == false).
                findFirst();
        if (!first.isPresent()) {
            transformRow(row);
        } else {
            this.flowListener.accept("Row ignored by filtering");
        }
        row.successfullyProcessed();
    }

    void transformRow(Row currentRow) {
        Row entryColumns = applyRowTransformations(this.beforeTransformations, currentRow);
        this.flowListener.accept("Pre Row transformations processed");
        applyExpressions(currentRow);
        this.flowListener.accept("Row expressions processed");
        columnTransformations(entryColumns);
        this.flowListener.accept("Column transformations processed");
        Row afterProcessed = applyRowTransformations(this.afterTransformations, entryColumns);
        if (afterProcessed == null) {
            return;
        }
        this.flowListener.accept("Post Row transformations processed: " + afterProcessed.getNumberOfColumns() + " entries");
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

    void sink(SinkTemplate sink, Map<String, Row> groupByDestinations) {
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
        final Function<Object, Object> function = this.columnTransformations.get(name);
        if (function != null) {
            this.flowListener.accept("Function: " + function + " found for name: " + name);
            return function.apply(value);
        } else {
            this.flowListener.accept("No function found for name: " + name);
            return value;
        }
    }

    void columnTransformations(Row entryColumns) {
        this.columnTransformations.forEach((k, v) -> entryColumns.transformColumn(k, v));
    }

    static Row applyRowTransformations(List<Function<Row, Row>> trafos, Row convertedColumns) {
        if (trafos == null || trafos.isEmpty()) {
            return convertedColumns;
        }
        final Function<Row, Row> composition = trafos.stream().
                reduce((i, j) -> i.andThen(j)).
                get();
        Row result = composition.apply(convertedColumns);
        if (result == null) {
            return null;
        } else {
            return result;
        }
    }

    public List<SinkTemplate> getSinks() {
        return sinks;
    }

    public static class Engine {

        private List<SinkTemplate> sinks;
        private SinkTemplate deadLetterQueue;
        private Source source;
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
        private boolean stopOnError;
        private Memory engineMemory;
        private Map<String, Object> bindings;

        public Engine() {
            this.sinks = new ArrayList<>();
            this.expressions = new ArrayList<>();
            this.filterExpressions = new ArrayList<>();
            this.entryFunctions = new HashMap<>();
            this.before = new ArrayList<>();
            this.after = new ArrayList<>();
            this.indexedFunctions = new HashMap<>();
            this.flowListener = f -> {
            };
            this.deadLetterQueue = new LogSink();
            this.stopOnError = true;
            this.engineMemory = new Memory();
            this.bindings = new HashMap<>();
        }

        public Engine homeScriptFolder(String baseFolder, Map<String, Object> bindings) {
            this.bindings = bindings;
            this.loader = FunctionScriptLoader.create(baseFolder, bindings);
            return this;
        }

        public Engine homeScriptFolder(String baseFolder) {
            this.loader = FunctionScriptLoader.create(baseFolder, null);
            return this;
        }

        public Engine from(Source source) {
            this.source = source;
            return this;
        }

        public Engine to(SinkTemplate sink) {
            if (this.sinks == null) {
                this.sinks = new ArrayList<>();
            }
            this.sinks.add(sink);
            return this;
        }

        public Engine dlq(SinkTemplate sink) {
            this.deadLetterQueue = sink;
            return this;
        }

        public Engine startWith(RowTransformer transformer) {
            transformer.init(this.bindings);
            this.before.add(transformer::execute);
            return this;
        }

        public Engine startWith(String scriptName) {
            final RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            rowTransformer.init(this.bindings);
            return startWith(rowTransformer);
        }

        public Engine startWithExpression(String scriptName) {
            this.expressions.add(scriptName);
            return this;
        }

        public Engine with(String columnName, Function<Object, Object> entryFunction) {
            this.entryFunctions.put(columnName, entryFunction);
            return this;
        }

        public Engine with(Memory engineMemory) {
            this.engineMemory = engineMemory;
            return this;
        }

        public Engine withColumnScript(String columnName, String scriptName) {
            Function<Object, Object> function = load(scriptName);
            return with(columnName, function);
        }

        public Engine withColumnExpression(String columnName, String scriptContent) {
            ColumnTransformer columnTransformer = this.loader.createFromScript(scriptContent);
            columnTransformer.init(this.bindings);
            return with(columnName, columnTransformer::execute);
        }

        Function<Object, Object> load(String scriptName) {
            ColumnTransformer columnTransformer = this.loader.getColumnTransformer(scriptName);
            columnTransformer.init(this.bindings);
            return columnTransformer::execute;
        }

        public Engine endWith(RowTransformer transformer) {
            transformer.init(this.bindings);
            this.after.add(transformer::execute);
            return this;

        }

        public Engine endWith(String scriptName) {
            RowTransformer rowTransformer = this.loader.getRowTransformer(scriptName);
            rowTransformer.init(this.bindings);
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

        public Engine continueOnError() {
            this.stopOnError = false;
            return this;
        }

        public Map<String, Object> getScriptEngineBindings() {
            if (this.loader == null) {
                return null;
            } else {
                return this.loader.getScriptEngineBindings();
            }
        }

        public Pump build() {
            return new Pump(source,
                    this.before, this.entryFunctions,
                    this.filterExpressions,
                    this.expressions,
                    this.after, this.sinks,
                    this.deadLetterQueue,
                    this.sql,
                    this.flowListener,
                    this.stopOnError,
                    this.engineMemory,
                    getScriptEngineBindings(),
                    this.params);
        }

        public Engine use(Pipeline pipeline) {
            homeScriptFolder(pipeline.getScriptsHome());
            this.source = pipeline.getSource();
            this.sinks = pipeline.getSinks();
            pipeline.getPreRowTransformers().forEach(t -> startWith(t::execute));
            List<ColumnTransformation> trafos = pipeline.getColumnTransformations();
            trafos.forEach(t -> {
                String name = t.getColumnName();
                if (name != null) {
                    if (t.isScript()) {
                        withColumnScript(name, t.getScriptNameOrContent());
                    } else {
                        withColumnExpression(name, t.getScriptNameOrContent());
                    }
                }
            });
            pipeline.getPostRowTransfomers().forEach(t -> endWith(t::execute));
            this.expressions = pipeline.getExpressions();
            this.filterExpressions = pipeline.getFilters();
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
