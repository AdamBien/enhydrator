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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class JDBCSource extends UnmanagedConnectionProvider implements Source {

    @XmlAttribute
    private String name;

    JDBCSource() {
        //JAXB requires a no-arg contructor
    }

    JDBCSource(String driver, String url, String user, String pwd) {
        super(driver, url, user, pwd);
    }

    @Override
    public Iterable<List<Entry>> query(String query, Object... params) {
        PreparedStatement stmt;
        try {
            stmt = this.connection.prepareStatement(query);
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
            JDBCSource source = new JDBCSource(this.driver, this.url, this.user, this.password);
            source.connect();
            return source;
        }
    }
}
