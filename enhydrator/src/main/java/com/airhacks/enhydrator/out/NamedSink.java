package com.airhacks.enhydrator.out;

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
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
//@XmlJavaTypeAdapter(JAXBInterfaceAdapter.class)
public abstract class NamedSink implements Sink {

    protected String name;

    public NamedSink(String name) {
        this.name = name;
    }

    public NamedSink() {

    }

    static String computeDefaultName(Class clazz) {
        String simpleName = clazz.getSimpleName();
        return Character.toString(simpleName.charAt(0)).toLowerCase() + simpleName.substring(1);
    }

    public String getName() {
        if (this.name == null) {
            this.name = computeDefaultName(this.getClass());
        }
        return this.name;
    }

}
