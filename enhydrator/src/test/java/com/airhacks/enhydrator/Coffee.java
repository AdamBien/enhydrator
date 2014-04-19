package com.airhacks.enhydrator;

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
