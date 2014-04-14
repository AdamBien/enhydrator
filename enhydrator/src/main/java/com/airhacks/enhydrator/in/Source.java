package com.airhacks.enhydrator.in;

import com.airhacks.enhydrator.db.JDBCConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author airhacks.com
 */
public class Source extends JDBCConnection {

    private Source(String driver, String url, String user, String pwd) {
        super(driver, url, user, pwd);
    }

    public Iterable<ResultSet> query(String sql, Object... params) {
        PreparedStatement stmt;
        try {
            stmt = this.connection.prepareStatement(sql);
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot prepare SQL statement", ex);
        }
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            try {
                stmt.setObject(i + 1, param);
            } catch (SQLException ex) {
                throw new IllegalStateException("Cannot set parameter (" + i + "," + param + ")", ex);
            }
        }
        return () -> {
            try {
                return new ResultSetIterator(stmt.executeQuery());
            } catch (SQLException ex) {
                throw new IllegalStateException("Cannot execute query: " + sql, ex);
            }
        };
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

        public Source newSource() {
            Source source = new Source(this.driver, this.url, this.user, this.password);
            source.connect();
            return source;
        }
    }
}
