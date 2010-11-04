/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.test;

import io.s4.persist.HashMapPersister;

public class TestHashMapPersister {

    public static void main(String[] args) {
        HashMapPersister hp = new HashMapPersister();
        hp.init();

        hp.set("mykey1", "Test1", 40);
        hp.set("mykey2", "Test2", 48);
        hp.set("mykey3", "Test2", -1);

        try {
            Thread.sleep(1);
        } catch (Exception e) {
        }

        System.out.println("mykey1: " + hp.get("mykey1"));
        System.out.println("mykey2: " + hp.get("mykey2"));
        System.out.println("mykey3: " + hp.get("mykey3"));

        System.out.println("Going to sleep...");
        try {
            Thread.sleep(41000);
        } catch (Exception e) {
        }
        System.out.println("Waking up");

        System.out.println("mykey1: " + hp.get("mykey1"));
        System.out.println("mykey2: " + hp.get("mykey2"));
        System.out.println("mykey3: " + hp.get("mykey3"));

        System.out.println("Going to sleep...");
        try {
            Thread.sleep(10000);
        } catch (Exception e) {
        }
        System.out.println("Waking up");

        System.out.println("mykey1: " + hp.get("mykey1"));
        System.out.println("mykey2: " + hp.get("mykey2"));
        System.out.println("mykey3: " + hp.get("mykey3"));
        System.out.println("cleanUp: " + hp.cleanOutGarbage());
    }
}