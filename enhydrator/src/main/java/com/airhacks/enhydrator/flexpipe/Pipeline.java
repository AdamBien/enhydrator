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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "jdbc-pipeline")
public class Pipeline {

    private String name;
    private Source source;

    @XmlElement(name = "scripts-home")
    private String scriptsHome;

    @XmlElement(name = "sql-query")
    private String sqlQuery;

    @XmlElement(name = "query-param")
    private List<Object> queryParams;
    private Sink sink;

    @XmlElement(name = "pre-row-transformer")
    private List<String> preRowTransformers;

    @XmlElement(name = "entry-transformation")
    private List<EntryTransformation> entryTransformations;

    @XmlElement(name = "post-row-transformer")
    private List<String> postRowTransfomers;

    @XmlElement(name = "filter")
    private List<String> filters;

    @XmlElement(name = "expression")
    private List<String> expressions;

    Pipeline() {
        this.preRowTransformers = new ArrayList<>();
        this.entryTransformations = new ArrayList<>();
        this.postRowTransfomers = new ArrayList<>();
        this.queryParams = new ArrayList<>();
        this.expressions = new ArrayList<>();
        this.filters = new ArrayList<>();
    }

    public Pipeline(String name, String scriptsHome, String sqlQuery, Source source, Sink sink) {
        this();
        this.preRowTransformers = new ArrayList<>();
        this.sqlQuery = sqlQuery;
        this.scriptsHome = scriptsHome;
        this.name = name;
        this.source = source;
        this.sink = sink;
    }

    public String getName() {
        return name;
    }

    public void addPreRowTransforation(String transformer) {
        this.preRowTransformers.add(transformer);
    }

    public void addEntryTransformation(EntryTransformation et) {
        this.entryTransformations.add(et);
    }

    public void addPostRowTransformation(String transformer) {
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

    public Source getSource() {
        return source;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public List<Object> getQueryParams() {
        return queryParams;
    }

    public Sink getSink() {
        return sink;
    }

    public List<String> getPreRowTransformers() {
        return preRowTransformers;
    }

    public List<EntryTransformation> getEntryTransformations() {
        return entryTransformations;
    }

    public List<String> getPostRowTransfomers() {
        return postRowTransfomers;
    }

    public List<String> getExpressions() {
        return expressions;
    }

    public String getScriptsHome() {
        return scriptsHome;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode(this.name);
        hash = 67 * hash + Objects.hashCode(this.source);
        hash = 67 * hash + Objects.hashCode(this.sqlQuery);
        hash = 67 * hash + Objects.hashCode(this.queryParams);
        hash = 67 * hash + Objects.hashCode(this.sink);
        hash = 67 * hash + Objects.hashCode(this.preRowTransformers);
        hash = 67 * hash + Objects.hashCode(this.entryTransformations);
        hash = 67 * hash + Objects.hashCode(this.postRowTransfomers);
        hash = 67 * hash + Objects.hashCode(this.expressions);
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
        if (!Objects.equals(this.sink, other.sink)) {
            return false;
        }
        if (!Objects.equals(this.preRowTransformers, other.preRowTransformers)) {
            return false;
        }
        if (!Objects.equals(this.entryTransformations, other.entryTransformations)) {
            return false;
        }
        if (!Objects.equals(this.postRowTransfomers, other.postRowTransfomers)) {
            return false;
        }
        if (!Objects.equals(this.expressions, other.expressions)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JDBCPipeline{" + "name=" + name + ", source=" + source + ", sqlQuery=" + sqlQuery + ", queryParams=" + queryParams + ", sink=" + sink + ", preRowTransformers=" + preRowTransformers + ", entryTransformations=" + entryTransformations + ", postRowTransfomers=" + postRowTransfomers + ", expressions=" + expressions + '}';
    }
}
