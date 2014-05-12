package com.airhacks.enhydrator.transform;

import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "target-mapping")
public class Mapping {

    private String targetSink;
    private String targetObject;

    public Mapping(String targetSink, String targetObject) {
        this.targetSink = targetSink;
        this.targetObject = targetObject;
    }

    public Mapping() {
    }

    public String getTargetSink() {
        return targetSink;
    }

    public String getTargetObject() {
        return targetObject;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.targetSink);
        hash = 41 * hash + Objects.hashCode(this.targetObject);
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
        final Mapping other = (Mapping) obj;
        if (!Objects.equals(this.targetSink, other.targetSink)) {
            return false;
        }
        if (!Objects.equals(this.targetObject, other.targetObject)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Mapping{" + "targetSink=" + targetSink + ", targetObject=" + targetObject + '}';
    }

}
