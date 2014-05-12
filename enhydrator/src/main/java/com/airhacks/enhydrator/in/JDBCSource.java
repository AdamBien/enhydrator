package com.airhacks.enhydrator.in;

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "jdbc-source")
public class JDBCSource implements Source {

    @XmlAttribute
    private String name;

    private UnmanagedConnectionProvider connectionProvider;

    JDBCSource() {
        //JAXB requires a no-arg contructor
    }

    JDBCSource(UnmanagedConnectionProvider connectionProvider) {
        this();
        this.connectionProvider = connectionProvider;
        this.connectionProvider.connect();
    }

    @Override
    public Iterable<Row> query(String query, Object... params) {
        PreparedStatement stmt;
        try {
            Connection connection = this.connectionProvider.get();
            stmt = connection.prepareStatement(query);
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot prepare SQL statement", ex);
        }
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            try {
                stmt.setObject(i + 1, param);
            } catch (SQLException ex) {
                throw new IllegalStateException("Cannot set parameter (" + i + "," + param + ") for query: " + query, ex);
            }
        }
        try {
            return new EntryIterable(new ResultSetIterator(stmt.executeQuery()));
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot execute query: " + query, ex);
        }
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 11 * hash + Objects.hashCode(this.name);
        hash = 11 * hash + Objects.hashCode(this.connectionProvider);
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
        final JDBCSource other = (JDBCSource) obj;
        if (!Objects.equals(this.name, other.name)) {
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

        public JDBCSource newSource() {
            JDBCSource source = new JDBCSource(new UnmanagedConnectionProvider(driver, url, user, password));
            return source;
        }
    }

    @Override
    public String toString() {
        return "JDBCSource{" + "name=" + name + ", connectionProvider=" + connectionProvider + '}';
    }

}
