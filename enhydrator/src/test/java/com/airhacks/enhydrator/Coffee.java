package com.airhacks.enhydrator;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 *
 * @author adam-bien.com
 */
@Entity
public class Coffee {

    @Id
    @GeneratedValue
    private long id;
    private String name;
    private int strength;
    private String countryOfOrigin;
    private Roast intensity;
    private String description;
    private String beans;

    public Coffee(String name, int strength, String countryOfOrigin, Roast intensity, String description, String beans) {
        this.name = name;
        this.strength = strength;
        this.countryOfOrigin = countryOfOrigin;
        this.intensity = intensity;
        this.description = description;
        this.beans = beans;
    }

    public Coffee() {
    }

}
