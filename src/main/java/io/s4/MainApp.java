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
package io.s4;

import io.s4.processor.PEContainer;
import io.s4.processor.ProcessingElement;
import io.s4.util.Clock;
import io.s4.util.EventClock;
import io.s4.util.S4Util;
import io.s4.util.Watcher;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;


public class MainApp {

    private static String coreHome = "../s4_core";
    private static String appsHome = "../s4_apps";
    private static String extsHome = "../s4_exts";

    public static void main(String args[]) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("corehome")
                                       .hasArg()
                                       .withDescription("core home")
                                       .create("c"));

        options.addOption(OptionBuilder.withArgName("appshome")
                                       .hasArg()
                                       .withDescription("applications home")
                                       .create("a"));

        options.addOption(OptionBuilder.withArgName("s4clock")
                                       .hasArg()
                                       .withDescription("s4 clock")
                                       .create("d"));

        options.addOption(OptionBuilder.withArgName("seedtime")
                                       .hasArg()
                                       .withDescription("event clock initialization time")
                                       .create("s"));        
        
        options.addOption(OptionBuilder.withArgName("extshome")
                                       .hasArg()
                                       .withDescription("extensions home")
                                       .create("e"));

        options.addOption(OptionBuilder.withArgName("instanceid")
                                       .hasArg()
                                       .withDescription("instance id")
                                       .create("i"));

        options.addOption(OptionBuilder.withArgName("configtype")
                                       .hasArg()
                                       .withDescription("configuration type")
                                       .create("t"));

        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = null;
        String clockType = "wall";

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException pe) {
            System.err.println(pe.getLocalizedMessage());
            System.exit(1);
        }

        int instanceId = -1;
        if (commandLine.hasOption("i")) {
            String instanceIdStr = commandLine.getOptionValue("i");
            try {
                instanceId = Integer.parseInt(instanceIdStr);
            } catch (NumberFormatException nfe) {
                System.err.println("Bad instance id: %s" + instanceIdStr);
                System.exit(1);
            }
        }

        if (commandLine.hasOption("c")) {
            coreHome = commandLine.getOptionValue("c");
        }

        if (commandLine.hasOption("a")) {
            appsHome = commandLine.getOptionValue("a");
        }
        
        if (commandLine.hasOption("d")) {
            clockType = commandLine.getOptionValue("d");
        }

        if (commandLine.hasOption("e")) {
            extsHome = commandLine.getOptionValue("e");
        }

        String configType = "typical";
        if (commandLine.hasOption("t")) {
            configType = commandLine.getOptionValue("t");
        }
        
        long seedTime = 0;
        if (commandLine.hasOption("s")) {
            seedTime = Long.parseLong(commandLine.getOptionValue("s"));
        }

        File coreHomeFile = new File(coreHome);
        if (!coreHomeFile.isDirectory()) {
            System.err.println("Bad core home: " + coreHome);
            System.exit(1);
        }

        File appsHomeFile = new File(appsHome);
        if (!appsHomeFile.isDirectory()) {
            System.err.println("Bad applications home: " + appsHome);
            System.exit(1);
        }

        if (instanceId > -1) {
            System.setProperty("instanceId", "" + instanceId);
        } else {
            System.setProperty("instanceId", "" + S4Util.getPID());
        }

        List loArgs = commandLine.getArgList();

        if (loArgs.size() < 1) {
            // System.err.println("No bean configuration file specified");
            // System.exit(1);
        }

        // String s4ConfigXml = (String) loArgs.get(0);
        // System.out.println("s4ConfigXml is " + s4ConfigXml);

        ClassPathResource propResource = new ClassPathResource("s4_core.properties");
        Properties prop = new Properties();
        if (propResource.exists()) {
            prop.load(propResource.getInputStream());
        } else {
            System.err.println("Unable to find s4_core.properties. It must be available in classpath");
            System.exit(1);
        }

        ApplicationContext coreContext = null;
        String configBase = coreHome + File.separatorChar + "conf"
                + File.separatorChar + configType;
        String configPath = "";
        List<String> coreConfigUrls = new ArrayList<String>(); 
        File configFile = null;

        // load clock configuration
        configPath = configBase + File.separatorChar + clockType + "_clock.xml";            
        coreConfigUrls.add(configPath);

        // load core config xml
        configPath = configBase + File.separatorChar + "s4_core_conf.xml";
        configFile = new File(configPath);
        if (!configFile.exists()) {
            System.err.printf("S4 core config file %s does not exist\n",
                    configPath);
            System.exit(1);
        }
		
        coreConfigUrls.add(configPath);
        String[] coreConfigFiles = new String[coreConfigUrls.size()];
        coreConfigUrls.toArray(coreConfigFiles);

        String[] coreConfigFileUrls = new String[coreConfigFiles.length];
        for (int i = 0; i < coreConfigFiles.length; i++) {
            coreConfigFileUrls[i] = "file:" + coreConfigFiles[i];
        }

        coreContext = new FileSystemXmlApplicationContext(coreConfigFileUrls, coreContext);
        ApplicationContext context = coreContext;        
        
        Clock s4Clock = (Clock) context.getBean("clock");
        if (s4Clock instanceof EventClock && seedTime > 0) {
            EventClock s4EventClock = (EventClock)s4Clock;
            s4EventClock.updateTime(seedTime);
            System.out.println("Intializing event clock time with seed time " + s4EventClock.getCurrentTime());
        }
        
        PEContainer peContainer = (PEContainer) context.getBean("peContainer");

        Watcher w = (Watcher) context.getBean("watcher");
        w.setConfigFilename(configPath);

        
        // load extension modules
        String[] configFileNames = getModuleConfigFiles(extsHome, prop);
        if (configFileNames.length > 0) {
            String[] configFileUrls = new String[configFileNames.length];
            for (int i = 0; i < configFileNames.length; i++) {
                configFileUrls[i] = "file:" + configFileNames[i];
            }
            context = new FileSystemXmlApplicationContext(configFileUrls,
                                                          context);
        }

        // load application modules
        configFileNames = getModuleConfigFiles(appsHome, prop);
        if (configFileNames.length > 0) {
            String[] configFileUrls = new String[configFileNames.length];
            for (int i = 0; i < configFileNames.length; i++) {
                configFileUrls[i] = "file:" + configFileNames[i];
            }
            context = new FileSystemXmlApplicationContext(configFileUrls,
                                                          context);
            // attach any beans that implement ProcessingElement to the PE
            // Container
            String[] processingElementBeanNames = context.getBeanNamesForType(ProcessingElement.class);
            for (String processingElementBeanName : processingElementBeanNames) {
                Object bean = context.getBean(processingElementBeanName);
                try {
                    Method getS4ClockMethod = bean.getClass().getMethod("getS4Clock");
    
                    if (getS4ClockMethod.getReturnType().equals(Clock.class)) {
                        if (getS4ClockMethod.invoke(bean) == null) {
                            Method setS4ClockMethod = bean.getClass().getMethod("setS4Clock", Clock.class);
                            setS4ClockMethod.invoke(bean, coreContext.getBean("clock"));
                        }
                    }
                }
                catch (NoSuchMethodException mnfe) {
                    // acceptable
                }
                System.out.println("Adding processing element with bean name "
                        + processingElementBeanName + ", id "
                        + ((ProcessingElement) bean).getId());
                peContainer.addProcessor((ProcessingElement) bean);
            }
        }  
    }

    /**
     * 
     * @param prop
     * @return
     */
    private static String[] getModuleConfigFiles(String moduleBase, Properties prop) {
        List<String> configFileList = new ArrayList<String>();
        File moduleBaseFile = new File(moduleBase);

        // list applications
        File[] moduleDirs = moduleBaseFile.listFiles();
        for (File moduleDir : moduleDirs) {
            if (moduleDir.isDirectory()) {
                String confFileName = moduleDir.getAbsolutePath() + "/"
                        + moduleDir.getName() + "_conf.xml";
                File appsConfFile = new File(confFileName);
                if (appsConfFile.exists()) {
                    configFileList.add(appsConfFile.getAbsolutePath());
                } else {
                    System.err.println("Invalid application: " + moduleDir);
                }
            }
        }
        String[] ret = new String[configFileList.size()];
        configFileList.toArray(ret);
        System.out.println(Arrays.toString(ret));
        return ret;
    }    
}
