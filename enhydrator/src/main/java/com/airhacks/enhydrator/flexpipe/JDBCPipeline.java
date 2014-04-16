package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import java.util.ArrayList;
import java.util.List;
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

    private List<String> preRowTransformers;
    private List<EntryTransformation> entryTransformations;
    private List<String> postRowTransfomers;

    JDBCPipeline() {
        this.preRowTransformers = new ArrayList<>();
        this.entryTransformations = new ArrayList<>();
        this.postRowTransfomers = new ArrayList<>();
    }

    public JDBCPipeline(String name, JDBCSource source, Sink sink) {
        this();
        this.preRowTransformers = new ArrayList<>();
        this.name = name;
        this.source = source;
        this.sink = sink;
    }

    @Override
    public String getName() {
        return name;
    }

    public void addPreRowTransforation(String transformer) {
        this.preRowTransformers.add(transformer);
    }

    public void addEntryTransformation(EntryTransformation et) {
        this.entryTransformations.add(et);
    }

    public void addPostRowTransformation(String transformer) {
        this.postRowTransfomers.add(transformer);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.source);
        hash = 59 * hash + Objects.hashCode(this.sink);
        hash = 59 * hash + Objects.hashCode(this.preRowTransformers);
        hash = 59 * hash + Objects.hashCode(this.entryTransformations);
        hash = 59 * hash + Objects.hashCode(this.postRowTransfomers);
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
        if (!Objects.equals(this.preRowTransformers, other.preRowTransformers)) {
            return false;
        }
        if (!Objects.equals(this.entryTransformations, other.entryTransformations)) {
            return false;
        }
        if (!Objects.equals(this.postRowTransfomers, other.postRowTransfomers)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JDBCPipeline{" + "name=" + name + ", source=" + source + ", sink=" + sink + ", preRowTransformers=" + preRowTransformers + ", entryTransformations=" + entryTransformations + ", postRowTransfomers=" + postRowTransfomers + '}';
    }

}
