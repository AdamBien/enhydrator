package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.flexpipe.RowTransformation;
import com.airhacks.enhydrator.in.Row;
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
        this.initialize();
    }

    void initialize() {
        this.loader = new FunctionScriptLoader(this.baseScriptFolder);
        this.rowTransformer = this.loader.getRowTransformer(this.scriptName);
    }

    void afterUnmarshal(Unmarshaller umarshaller, Object parent) {
        System.out.println("afterUnmarschal called: " + toString());
        this.initialize();
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
