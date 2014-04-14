package com.airhacks.enhydrator;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 *
 * @author adam-bien.com
 */
@Entity
@Table(name = "DEVELOPER_DRINK")
public class DeveloperDrink {

    @Id
    @GeneratedValue
    private long id;
    private String name;
    private int strength;
    private String countryOfOrigin;
    private Roast intensity;
    private String description;
    private String beans;

    public DeveloperDrink(String name, int strength, String countryOfOrigin, Roast intensity, String description, String beans) {
        this.name = name;
        this.strength = strength;
        this.countryOfOrigin = countryOfOrigin;
        this.intensity = intensity;
        this.description = description;
        this.beans = beans;
    }

    public DeveloperDrink() {
    }

}
