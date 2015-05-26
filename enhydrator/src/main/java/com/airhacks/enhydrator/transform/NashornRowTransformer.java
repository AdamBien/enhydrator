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
import com.airhacks.enhydrator.flexpipe.RowTransformation;
import com.airhacks.enhydrator.in.Row;
import java.util.Map;
import java.util.Objects;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlRootElement(name = "javascript-row-transformer")
@XmlAccessorType(XmlAccessType.FIELD)
public class NashornRowTransformer extends RowTransformation {

    private String scriptName;
    private String baseScriptFolder;
    @XmlTransient
    private FunctionScriptLoader loader;
    @XmlTransient
    private RowTransformer rowTransformer;

    public NashornRowTransformer() {
    }

    public NashornRowTransformer(String baseScriptFolder, String scriptName) {
        this.scriptName = scriptName;
        this.baseScriptFolder = baseScriptFolder;
    }

    @Override
    public void init(Map<String, Object> scriptEngineBindings) {
        this.loader = FunctionScriptLoader.create(this.baseScriptFolder, scriptEngineBindings);
        this.rowTransformer = this.loader.getRowTransformer(this.scriptName);
    }

    void afterUnmarshal(Unmarshaller umarshaller, Object parent) {
        System.out.println("afterUnmarschal called: " + toString());
    }

    public void setBaseScriptFolder(String baseScriptFolder) {
        this.baseScriptFolder = baseScriptFolder;
    }

    @Override
    public Row execute(Row input) {
        return this.rowTransformer.execute(input);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.scriptName);
        hash = 37 * hash + Objects.hashCode(this.baseScriptFolder);
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
        final NashornRowTransformer other = (NashornRowTransformer) obj;
        if (!Objects.equals(this.scriptName, other.scriptName)) {
            return false;
        }
        if (!Objects.equals(this.baseScriptFolder, other.baseScriptFolder)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "NashornRowTransformer{" + "scriptName=" + scriptName + ", baseScriptFolder=" + baseScriptFolder + ", loader=" + loader + ", rowTransformer=" + rowTransformer + '}';
    }

}
