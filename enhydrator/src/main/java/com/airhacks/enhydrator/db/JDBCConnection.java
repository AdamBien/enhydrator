package com.airhacks.enhydrator.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;
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
public class JDBCConnection {

    protected String user;
    protected String pwd;
    protected String url;
    protected String driver;
    @XmlTransient
    protected Connection connection;

    @XmlTransient
    protected Consumer<String> LOG;

    public JDBCConnection() {
        this.LOG = l -> {
        };
    }

    protected JDBCConnection(String driver, String url, String user, String pwd) {
        this();
        this.driver = driver;
        this.user = user;
        this.pwd = pwd;
        this.url = url;
    }

    protected void connect() {
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
        final JDBCConnection other = (JDBCConnection) obj;
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
