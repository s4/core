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
package io.s4.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.log4j.Logger;

public class PreprodLogger {

    String filenamePrefix;
    File file;
    FileOutputStream fos;

    private DecimalFormat formatter = new DecimalFormat("0000");

    public void setFilenamePrefix(String filenamePrefix) {
        this.filenamePrefix = filenamePrefix;
    }

    public PreprodLogger() {
    }

    public void openNewFile() {
        for (int count = 1; true; count++) {
            String countString = formatter.format(count);
            String filename = filenamePrefix + "." + countString + ".txt";
            file = new File(filename);
            if (!file.exists()) {
                break;
            }
        }

        try {
            fos = new FileOutputStream(file);
        } catch (IOException ioe) {
            Logger.getLogger("s4")
                  .error("Some sort of exception opening event logging file",
                         ioe);
            return;
        }
    }
}
