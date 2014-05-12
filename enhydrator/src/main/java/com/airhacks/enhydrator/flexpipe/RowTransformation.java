package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.transform.RowTransformer;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "row-transformation")
public abstract class RowTransformation implements RowTransformer {

}
