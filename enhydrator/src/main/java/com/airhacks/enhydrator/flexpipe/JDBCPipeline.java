package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "jdbc-pipeline")
public class JDBCPipeline implements Pipeline {

    private String name;
    private JDBCSource source;
    private Sink sink;

    JDBCPipeline() {
    }

    public JDBCPipeline(String name, JDBCSource source, Sink sink) {
        this.name = name;
        this.source = source;
        this.sink = sink;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.name);
        hash = 97 * hash + Objects.hashCode(this.source);
        hash = 97 * hash + Objects.hashCode(this.sink);
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
        final JDBCPipeline other = (JDBCPipeline) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.source, other.source)) {
            return false;
        }
        if (!Objects.equals(this.sink, other.sink)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JDBCPipeline{" + "name=" + name + ", source=" + source + ", sink=" + sink + '}';
    }

}
