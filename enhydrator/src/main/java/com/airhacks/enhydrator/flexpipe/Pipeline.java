package com.airhacks.enhydrator.flexpipe;

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
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.out.Sink;
import com.airhacks.enhydrator.out.NamedSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "pipeline")
public class Pipeline {

    private String name;
    private Source source;

    @XmlElement(name = "stop-on-error")
    private boolean stopOnError;

    @XmlElement(name = "scripts-home")
    private String scriptsHome;

    @XmlElement(name = "sql-query")
    private String sqlQuery;

    @XmlElement(name = "query-param")
    private List<Object> queryParams;

    @XmlElement(name = "filter")
    private List<String> filters;

    @XmlElement(name = "pre-row-transformation")
    private List<RowTransformation> preRowTransformers;

    @XmlElement(name = "column-transformation")
    private List<ColumnTransformation> columnTransformations;

    @XmlElement(name = "expression")
    private List<String> expressions;

    @XmlElement(name = "post-row-transformation")
    private List<RowTransformation> postRowTransfomers;

    @XmlElement(name = "sink")
    private List<NamedSink> sinks;

    Pipeline() {
        this.preRowTransformers = new ArrayList<>();
        this.columnTransformations = new ArrayList<>();
        this.postRowTransfomers = new ArrayList<>();
        this.queryParams = new ArrayList<>();
        this.expressions = new ArrayList<>();
        this.filters = new ArrayList<>();
        this.stopOnError = false;
    }

    public Pipeline(String name, String scriptsHome, String sqlQuery, Source source) {
        this();
        this.preRowTransformers = new ArrayList<>();
        this.sinks = new ArrayList<>();
        this.sqlQuery = sqlQuery;
        this.scriptsHome = scriptsHome;
        this.name = name;
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public void addPreRowTransformation(RowTransformation transformer) {
        this.preRowTransformers.add(transformer);
    }

    public void addEntryTransformation(ColumnTransformation et) {
        this.columnTransformations.add(et);
    }

    public void addPostRowTransformation(RowTransformation transformer) {
        this.postRowTransfomers.add(transformer);
    }

    public void addExpression(String expression) {
        this.expressions.add(expression);
    }

    public void addFilter(String filter) {
        this.filters.add(filter);
    }

    public void addQueryParam(Object value) {
        this.queryParams.add(value);
    }

    public void addSink(NamedSink sink) {
        this.sinks.add(sink);
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public List<Object> getQueryParams() {
        return queryParams;
    }

    public List<Sink> getSinks() {
        return sinks.stream().map(s -> (Sink) s).collect(Collectors.toList());
    }

    public List<RowTransformation> getPreRowTransformers() {
        return preRowTransformers;
    }

    public List<ColumnTransformation> getColumnTransformations() {
        return columnTransformations;
    }

    public List<RowTransformation> getPostRowTransfomers() {
        return postRowTransfomers;
    }

    public List<String> getExpressions() {
        return expressions;
    }

    public List<String> getFilters() {
        return filters;
    }

    public String getScriptsHome() {
        return scriptsHome;
    }

    public void setScriptsHome(String scriptsHome) {
        this.scriptsHome = scriptsHome;
    }

    public void setStopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode(this.name);
        hash = 67 * hash + Objects.hashCode(this.source);
        hash = 67 * hash + Objects.hashCode(this.sqlQuery);
        hash = 67 * hash + Objects.hashCode(this.queryParams);
        hash = 67 * hash + Objects.hashCode(this.sinks);
        hash = 67 * hash + Objects.hashCode(this.preRowTransformers);
        hash = 67 * hash + Objects.hashCode(this.columnTransformations);
        hash = 67 * hash + Objects.hashCode(this.postRowTransfomers);
        hash = 67 * hash + Objects.hashCode(this.expressions);
        hash = 67 * hash + Objects.hashCode(this.filters);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Pipeline other = (Pipeline) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.source, other.source)) {
            return false;
        }
        if (!Objects.equals(this.sqlQuery, other.sqlQuery)) {
            return false;
        }
        if (!Objects.equals(this.queryParams, other.queryParams)) {
            return false;
        }
        if (!Objects.equals(this.sinks, other.sinks)) {
            return false;
        }
        if (!Objects.equals(this.preRowTransformers, other.preRowTransformers)) {
            return false;
        }
        if (!Objects.equals(this.columnTransformations, other.columnTransformations)) {
            return false;
        }
        if (!Objects.equals(this.postRowTransfomers, other.postRowTransfomers)) {
            return false;
        }
        if (!Objects.equals(this.expressions, other.expressions)) {
            return false;
        }
        if (!Objects.equals(this.filters, other.filters)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "JDBCPipeline{" + "name=" + name + ", source=" + source + ", sqlQuery=" + sqlQuery + ", queryParams=" + queryParams + ", sink=" + sinks + ", preRowTransformers=" + preRowTransformers + ", entryTransformations=" + columnTransformations + ", postRowTransfomers=" + postRowTransfomers + ", expressions=" + expressions + '}';
    }

}
