package com.airhacks.enhydrator.db;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class UnmanagedConnectionProvider implements Supplier<Connection> {

    protected String user;
    protected String pwd;
    protected String url;
    protected String driver;
    @XmlTransient
    protected Connection connection;

    @XmlTransient
    protected Consumer<String> LOG;

    public UnmanagedConnectionProvider() {
        this.LOG = l -> {
        };
    }

    public UnmanagedConnectionProvider(String driver, String url, String user, String pwd) {
        this();
        this.driver = driver;
        this.user = user;
        this.pwd = pwd;
        this.url = url;
    }

    public void connect() {
        try {
            Class.forName(this.driver);
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("Cannot load driver", ex);
        }
        if (this.user != null && !this.user.isEmpty()) {
            try {
                this.connection = DriverManager.getConnection(this.url, this.user, this.pwd);
            } catch (SQLException ex) {
                throw new IllegalStateException("Cannot fetch connection", ex);
            }
        } else {
            try {
                this.connection = DriverManager.getConnection(this.url);
            } catch (SQLException ex) {
                throw new IllegalStateException("Cannot fetch connection", ex);
            }
        }
    }

    @Override
    public Connection get() {
        return this.connection;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.user);
        hash = 79 * hash + Objects.hashCode(this.pwd);
        hash = 79 * hash + Objects.hashCode(this.url);
        hash = 79 * hash + Objects.hashCode(this.driver);
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
        final UnmanagedConnectionProvider other = (UnmanagedConnectionProvider) obj;
        if (!Objects.equals(this.user, other.user)) {
            return false;
        }
        if (!Objects.equals(this.pwd, other.pwd)) {
            return false;
        }
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Objects.equals(this.driver, other.driver)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JDBCConnection{" + "user=" + user + ", pwd=" + pwd + ", url=" + url + ", driver=" + driver + '}';
    }

}
