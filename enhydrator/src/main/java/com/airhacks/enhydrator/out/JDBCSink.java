package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.db.JDBCConnection;
import com.airhacks.enhydrator.in.Entry;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author airhacks.com
 */
public class JDBCSink extends JDBCConnection implements Sink {

    private String targetTable;
    private Statement statement;

    JDBCSink(String driver, String url, String user, String pwd, String table) {
        super(driver, url, user, pwd);
        this.targetTable = table;
    }

    @Override
    public void init() {
        try {
            this.statement = this.connection.createStatement();
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot create statement " + ex.getMessage(), ex);
        }
    }

    @Override
    public void processRow(List<Entry> entries) {
        try {
            this.statement.execute(generateInsertStatement(entries));
        } catch (SQLException ex) {
            throw new IllegalStateException("Cannot insert entry: " + ex.getMessage(), ex);
        }
    }

    String generateInsertStatement(List<Entry> entries) {
        return "INSERT INTO " + this.targetTable + " VALUES (" + valueList(entries) + ")";
    }

    static String valueList(List<Entry> entries) {
        return (String) entries.stream().
                map(e -> e.getValue()).
                reduce((t, u) -> t + "," + u).
                get();
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(JDBCSink.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            this.statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(JDBCSink.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static class Configuration {

        protected String url;
        protected String driver;
        protected String user;
        protected String password;
        protected String targetTable;

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

        public Sink newSink() {
            JDBCSink source = new JDBCSink(this.driver, this.url, this.user, this.password, this.targetTable);
            source.connect();
            return source;
        }
    }

}
