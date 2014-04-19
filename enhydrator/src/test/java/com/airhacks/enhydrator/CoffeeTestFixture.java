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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class CoffeeTestFixture {

    private static final Logger LOG = Logger.getLogger(CoffeeTestFixture.class.getName());

    public static void insertCoffee(String name, int strength, String countryOfOrigin, Roast intensity, String description, String beans) {
        Function<EntityManager, Void> execution = (em) -> {
            em.merge(
                    new Coffee(name, strength, countryOfOrigin, intensity, description, beans));
            return null;
        };
        perform("from", execution);
    }

    public static void deleteTables() {
        Function<EntityManager, Object> from = (em) -> {
            int deleted = em.createQuery("DELETE FROM Coffee").executeUpdate();
            LOG.info(deleted + " deleted from coffee");
            return null;
        };
        Function<EntityManager, Object> to = (em) -> {
            int deleted = em.createQuery("DELETE FROM DeveloperDrink").executeUpdate();
            LOG.info(deleted + " deleted from DEVELOPER_DRINK");
            return null;
        };
        perform("from", from);
        perform("to", to);
    }

    public static List<DeveloperDrink> all() {
        Function<EntityManager, List<DeveloperDrink>> execution = (em) -> {
            return em.createNamedQuery("DeveloperDrink.all", DeveloperDrink.class).getResultList();
        };
        return (List<DeveloperDrink>) perform("to", execution);
    }

    public static Object perform(String unitName, Function<EntityManager, ?> statement) {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(unitName);
        EntityManager em = emf.createEntityManager();
        EntityTransaction et = em.getTransaction();
        et.begin();
        Object result = statement.apply(em);
        et.commit();
        return result;

    }

}
