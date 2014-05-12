package com.airhacks.enhydrator.transform;

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
