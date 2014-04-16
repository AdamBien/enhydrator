package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.in.JDBCSource;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class JDBCPipeline implements Pipeline {

    private String name;
    private JDBCSource source;

    JDBCPipeline() {
    }

    public JDBCPipeline(String name, JDBCSource source) {
        this.name = name;
        this.source = source;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 11 * hash + Objects.hashCode(this.name);
        hash = 11 * hash + Objects.hashCode(this.source);
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
        return true;
    }

    @Override
    public String toString() {
        return "JDBCPipeline{" + "name=" + name + ", source=" + source + '}';
    }

}
