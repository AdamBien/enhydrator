package com.airhacks.enhydrator;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author airhacks.com
 */
public class CoffeeTestFixture {

    public static void insertCoffee(String name, int strength, String countryOfOrigin, Roast intensity, String description, String beans) {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("coffees");
        EntityManager em = emf.createEntityManager();
        EntityTransaction et = em.getTransaction();
        et.begin();
        em.merge(
                new Coffee(name, strength, countryOfOrigin, intensity, description, beans));
        et.commit();
    }

    public static void deleteTable() {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("coffees");
        EntityManager em = emf.createEntityManager();
        EntityTransaction et = em.getTransaction();
        et.begin();
        em.createQuery("DELETE FROM Coffee").executeUpdate();
        et.commit();

    }
}
