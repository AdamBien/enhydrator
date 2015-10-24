package com.airhacks.enhydrator.out;

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
import com.airhacks.enhydrator.db.UnmanagedConnectionProvider;
import com.airhacks.enhydrator.in.Row;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "jdbc-sink")
public class JDBCSink extends NamedSink {

    private static final char ESC_CHAR = '\'';

    @XmlElement(name = "target-table")
    private String targetTable;
    @XmlTransient
    private Statement statement;
    private UnmanagedConnectionProvider connectionProvider;

    @XmlTransient
    private Connection connection;

    @XmlTransient
    protected Consumer<String> LOG;

    public JDBCSink() {
        this.LOG = l -> {
        };

    }

    JDBCSink(UnmanagedConnectionProvider connection, String table) {
        this();
        this.connectionProvider = connection;
        this.targetTable = table;
    }

    @Override
    public void init() {
        try {
            this.connectionProvider.connect();
            this.connection = this.connectionProvider.get();
            this.statement = connection.createStatement();
            LOG.accept("#init() Statement created");
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot create statement " + ex.getMessage(), ex);
        }
        try {
            this.connection.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot commit connection: " + ex.getMessage(), ex);
        }

    }

    @Override
    public void processRow(Row columns) {
        if (columns == null || columns.isEmpty()) {
            this.LOG.accept("Nothing to do -> empty entry list");
            return;
        }
        try {
            final String insertSQL = generateInsertStatement(columns);
            LOG.accept("#processRow(): " + insertSQL);
            this.statement.execute(insertSQL);
            LOG.accept("#processRow() executed!");
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot insert entry: " + ex.getMessage(), ex);
        }
    }

    String generateInsertStatement(Row entries) {
        return "INSERT INTO " + this.targetTable + " (" + columnList(entries)
                + ") VALUES (" + valueList(entries) + ")";
    }

    static String valueList(Row row) {
        if (row == null || row.isEmpty()) {
            return null;
        }
        return (String) row.getColumnNames().
                stream().
                map(e -> asInsertSQL(row, e)).
                reduce((t, u) -> t + "," + u).
                get();
    }

    static String columnList(Row entries) {
        if (entries == null || entries.isEmpty()) {
            return null;
        }
        return (String) entries.getColumnNames().
                stream().
                reduce((t, u) -> t + "," + u).
                get();
    }

    static String asInsertSQL(Row row, String columnName) {
        if (row.isString(columnName)) {
            return escape(row.getColumnValue(columnName));
        } else {
            return String.valueOf(row.getColumnValue(columnName));
        }
    }

    static String escape(Object t) {
        return ESC_CHAR + String.valueOf(t) + ESC_CHAR;
    }

    @Override
    public void close() {
        try {
            this.connection.close();
            LOG.accept("#close() Connection successfully closed");

        } catch (SQLException ex) {
            Logger.getLogger(JDBCSink.class
                    .getName()).log(Level.SEVERE, null, ex);
        }

        try {
            this.statement.close();

        } catch (SQLException ex) {
            Logger.getLogger(JDBCSink.class
                    .getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.name);
        hash = 37 * hash + Objects.hashCode(this.targetTable);
        hash = 37 * hash + Objects.hashCode(this.connectionProvider);
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
        final JDBCSink other = (JDBCSink) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.targetTable, other.targetTable)) {
            return false;
        }
        if (!Objects.equals(this.connectionProvider, other.connectionProvider)) {
            return false;
        }
        return true;

    }

    public static class Configuration {

        protected String url;
        protected String driver;
        protected String user;
        protected String password;
        protected String targetTable;
        protected String name;

        public Configuration driver(String driver) {
            this.driver = driver;
            return this;
        }

        public Configuration url(String url) {
            this.url = url;
            return this;
        }

        public Configuration user(String user) {
            this.user = user;
            return this;
        }

        public Configuration password(String password) {
            this.password = password;
            return this;
        }

        public Configuration targetTable(String table) {
            this.targetTable = table;
            return this;
        }

        public Configuration name(String name) {
            this.name = name;
            return this;
        }

        public NamedSink newSink() {
            JDBCSink source = new JDBCSink(new UnmanagedConnectionProvider(driver, url, user, password), this.targetTable);
            if (this.name != null) {
                source.name = this.name;
            }
            return source;
        }

    }

}
