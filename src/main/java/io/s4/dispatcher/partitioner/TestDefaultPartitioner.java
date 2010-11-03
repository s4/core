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
package io.s4.dispatcher.partitioner;

import java.util.ArrayList;
import java.util.List;

public class TestDefaultPartitioner {
    public static void main(String[] args) {
        DefaultPartitioner dp1 = new DefaultPartitioner();
        DefaultPartitioner dp2 = new DefaultPartitioner();
        dp1.setDebug(true);
        dp1.setHashKey(new String[] { "list1/val1", "list1/val2", "query" });
        dp1.setHasher(new DefaultHasher());

        dp2.setDebug(true);
        dp2.setHashKey(new String[] { "user" });
        dp2.setHasher(new DefaultHasher());

        TopLevel tl1 = new TopLevel();
        tl1.setQuery("Hello there");
        tl1.setUser("spitzer");

        for (int i = 0; i < 4; i++) {
            Nested n = new Nested();
            n.setVal1(i + 77);
            n.setVal2(i / 10.7);
            tl1.addNested(n);
        }

        dp1.partition("test", tl1, 4);
        dp2.partition("test", tl1, 4);

    }

    static class TopLevel {
        private String query;
        private List<Nested> list1 = new ArrayList<Nested>();
        private String user;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public List<Nested> getList1() {
            return list1;
        }

        public void setList1(List<Nested> list1) {
            this.list1 = list1;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public void addNested(Nested nested) {
            list1.add(nested);
        }
    }

    static class Nested {
        long val1;
        double val2;

        public Nested() {

        }

        public long getVal1() {
            return val1;
        }

        public void setVal1(long val1) {
            this.val1 = val1;
        }

        public double getVal2() {
            return val2;
        }

        public void setVal2(double val2) {
            this.val2 = val2;
        }
    }
}
