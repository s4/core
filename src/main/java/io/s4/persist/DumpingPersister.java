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
package io.s4.persist;

import io.s4.processor.OutputFormatter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class DumpingPersister extends ConMapPersister implements Runnable {
    private String dumpFilePrefix;
    private Map<String, OutputFormatter> regexFormatter;
    private Pattern[] patterns;
    private OutputFormatter[] formatters;
    private long outputTimeBoundary;

    public void setDumpFilePrefix(String dumpFilePrefix) {
        this.dumpFilePrefix = dumpFilePrefix;
    }

    public void setRegexFormatter(Map<String, OutputFormatter> regexFormatter) {
        this.regexFormatter = regexFormatter;
    }

    public void setOutputTimeBoundary(long outputTimeBoundary) {
        this.outputTimeBoundary = outputTimeBoundary;
    }

    public void init() {
        super.init();

        Set<String> regexes = regexFormatter.keySet();
        patterns = new Pattern[regexes.size()];
        formatters = new OutputFormatter[regexes.size()];

        int i = 0;
        for (String regex : regexes) {
            patterns[i] = Pattern.compile(regex);
            formatters[i] = regexFormatter.get(regex);
            i++;
        }

        Thread t = new Thread(this);
        t.start();
    }

    public void run() {
        long boundaryInMillis = outputTimeBoundary * 1000;
        long currentTime = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            long currentBoundary = (currentTime / boundaryInMillis)
                    * boundaryInMillis;
            long interval = ((currentBoundary + boundaryInMillis) - System.currentTimeMillis());
            if (interval > 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }

            currentTime = System.currentTimeMillis();

            try {
                output();
            } catch (Exception e) {
                Logger.getLogger("s4").error("Exception dumping persister", e);
            }
        }
    }

    public void output() {
        File file = new File(dumpFilePrefix + UUID.randomUUID().toString());
        Logger.getLogger("s4").info("Dumping to " + file);
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter dumpWriter = null;

        try {
            fos = new FileOutputStream(file);
            osw = new OutputStreamWriter(fos);
            dumpWriter = new BufferedWriter(osw);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        int savedPriority = Thread.currentThread().getPriority();
        try {

            Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
            Set<String> keys = new HashSet<String>();
            for (String key : this.keySet()) {
                keys.add(key);
            }

            for (String key : keys) {
                Object value = this.get(key);
                if (value == null) {
                    continue;
                }
                for (int patternId = 0; patternId < patterns.length; patternId++) {
                    Matcher m = patterns[patternId].matcher(key);
                    if (m.matches()) {
                        Object formattedValue = formatters[patternId].format(value);
                        dumpWriter.write(key + " = " + formattedValue + "\n");
                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setPriority(savedPriority);
            try {
                dumpWriter.close();
            } catch (Exception e) {
            }
            try {
                osw.close();
            } catch (Exception e) {
            }
            try {
                fos.close();
            } catch (Exception e) {
            }
        }
    }
}
