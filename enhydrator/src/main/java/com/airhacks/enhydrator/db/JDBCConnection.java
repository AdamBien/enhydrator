package com.airhacks.enhydrator.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author airhacks.com
 */
public class JDBCConnection {

    protected String user;
    protected String pwd;
    protected String url;
    protected String driver;
    protected Connection connection;

    protected JDBCConnection(String driver, String url, String user, String pwd) {
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

}
