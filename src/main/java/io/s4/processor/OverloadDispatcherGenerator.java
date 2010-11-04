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
package io.s4.processor;

import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.bcel.Constants;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.BranchInstruction;
import org.apache.bcel.generic.ClassGen;
import org.apache.bcel.generic.ConstantPoolGen;
import org.apache.bcel.generic.INSTANCEOF;
import org.apache.bcel.generic.InstructionFactory;
import org.apache.bcel.generic.InstructionHandle;
import org.apache.bcel.generic.InstructionList;
import org.apache.bcel.generic.MethodGen;
import org.apache.bcel.generic.ObjectType;
import org.apache.bcel.generic.Type;

public class OverloadDispatcherGenerator {
    private List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
    private Class<?> targetClass;
    private boolean forSlot = false;
    private ObjectType abstractWindowingPEType = new ObjectType("io.s4.processor.AbstractWindowingPE");
    private String classDumpFile;

    public void setClassDumpFile(String classDumpFile) {
        this.classDumpFile = classDumpFile;
    }

    public OverloadDispatcherGenerator(Class targetClass) {
        this(targetClass, false);
    }

    public OverloadDispatcherGenerator(Class targetClass, boolean forSlot) {
        this.targetClass = targetClass;
        this.forSlot = forSlot;

        for (Method method : targetClass.getMethods()) {
            if (method.getName().equals("processEvent")
                    && method.getReturnType().equals(Void.TYPE)) {
                this.addHierarchy((Class<?>) method.getParameterTypes()[0]);
            }
        }
        Collections.sort(hierarchies);
    }

    public void addHierarchy(Class<?> clazz) {
        hierarchies.add(new Hierarchy(clazz));
    }

    public Class<Object> generate() {
        Random rand = new Random(System.currentTimeMillis());
        String dispatcherClassName = "OverloadDispatcher"
                + (Math.abs(rand.nextInt() % 3256));

        String interfaceName = "io.s4.processor.OverloadDispatcher";
        if (forSlot) {
            interfaceName = "io.s4.processor.OverloadDispatcherSlot";
        }

        ClassGen cg = new ClassGen(dispatcherClassName,
                                   "java.lang.Object",
                                   dispatcherClassName + ".java",
                                   Constants.ACC_PUBLIC | Constants.ACC_SUPER,
                                   new String[] { interfaceName });
        ConstantPoolGen cp = cg.getConstantPool();
        InstructionFactory instFactory = new InstructionFactory(cg, cp);

        InstructionList il = new InstructionList();

        // build constructor method for new class
        MethodGen constructor = new MethodGen(Constants.ACC_PUBLIC,
                                              Type.VOID,
                                              Type.NO_ARGS,
                                              new String[] {},
                                              "<init>",
                                              dispatcherClassName,
                                              il,
                                              cp);
        il.append(InstructionFactory.createLoad(Type.OBJECT, 0));
        il.append(instFactory.createInvoke("java.lang.Object",
                                           "<init>",
                                           Type.VOID,
                                           Type.NO_ARGS,
                                           Constants.INVOKESPECIAL));

        il.append(InstructionFactory.createReturn(Type.VOID));
        constructor.setMaxStack();
        constructor.setMaxLocals();
        cg.addMethod(constructor.getMethod());
        il.dispose();

        // build dispatch method
        il = new InstructionList();

        Type[] dispatchArgumentTypes = null;
        String[] dispatchArgumentNames = null;
        int postArgumentVariableSlot = 3;
        if (forSlot) {
            dispatchArgumentTypes = new Type[] { ObjectType.OBJECT,
                    ObjectType.OBJECT, ObjectType.LONG, abstractWindowingPEType };
            dispatchArgumentNames = new String[] { "slot", "event", "slotTime",
                    "pe" };
            postArgumentVariableSlot = 6;
        } else {
            dispatchArgumentTypes = new Type[] { ObjectType.OBJECT,
                    ObjectType.OBJECT };
            dispatchArgumentNames = new String[] { "pe", "event" };

        }

        MethodGen method = new MethodGen(Constants.ACC_PUBLIC,
                                         Type.VOID,
                                         dispatchArgumentTypes,
                                         dispatchArgumentNames,
                                         "dispatch",
                                         dispatcherClassName,
                                         il,
                                         cp);

        List<InstructionHandle> targetInstructions = new ArrayList<InstructionHandle>();
        List<BranchInstruction> branchInstructions = new ArrayList<BranchInstruction>();
        List<BranchInstruction> gotoInstructions = new ArrayList<BranchInstruction>();

        ObjectType peType = new ObjectType(targetClass.getName());

        il.append(InstructionFactory.createLoad(Type.OBJECT, 1));
        il.append(instFactory.createCheckCast(peType));
        il.append(InstructionFactory.createStore(peType,
                                                 postArgumentVariableSlot));

        for (int i = 0; i < hierarchies.size(); i++) {
            Hierarchy hierarchy = hierarchies.get(i);

            ObjectType hierarchyTop = new ObjectType(hierarchy.getTop()
                                                              .getName());

            InstructionHandle ih = il.append(InstructionFactory.createLoad(Type.OBJECT,
                                                                           2));
            if (i > 0) {
                targetInstructions.add(ih);
            }

            il.append(new INSTANCEOF(cp.addClass(hierarchyTop)));
            BranchInstruction bi = InstructionFactory.createBranchInstruction(Constants.IFEQ,
                                                                              null);
            il.append(bi);
            branchInstructions.add(bi);

            il.append(InstructionFactory.createLoad(peType,
                                                    postArgumentVariableSlot));
            il.append(InstructionFactory.createLoad(hierarchyTop, 2));
            il.append(instFactory.createCheckCast(hierarchyTop));
            if (forSlot) {
                il.append(InstructionFactory.createLoad(ObjectType.LONG, 3));
                il.append(InstructionFactory.createLoad(abstractWindowingPEType,
                                                        5));
            }

            Type[] argumentTypes = null;
            if (forSlot) {
                argumentTypes = new Type[] { hierarchyTop, ObjectType.LONG,
                        abstractWindowingPEType };
            } else {
                argumentTypes = new Type[] { hierarchyTop };
            }
            il.append(instFactory.createInvoke(targetClass.getName(),
                                               "processEvent",
                                               Type.VOID,
                                               argumentTypes,
                                               Constants.INVOKEVIRTUAL));

            // no branch needed for last check
            if (i < (hierarchies.size() - 1)) {
                bi = InstructionFactory.createBranchInstruction(Constants.GOTO,
                                                                null);
                il.append(bi);
                gotoInstructions.add(bi);
            }
        }

        InstructionHandle returnInstruction = il.append(InstructionFactory.createReturn(Type.VOID));

        for (int i = 0; i < targetInstructions.size(); i++) {
            branchInstructions.get(i).setTarget(targetInstructions.get(i));
        }

        branchInstructions.get(branchInstructions.size() - 1)
                          .setTarget(returnInstruction);

        for (BranchInstruction gotoInstruction : gotoInstructions) {
            gotoInstruction.setTarget(returnInstruction);
        }

        method.setMaxStack();
        method.setMaxLocals();
        cg.addMethod(method.getMethod());
        il.dispose();

        JavaClass jc = cg.getJavaClass();
        OverloadDispatcherClassLoader cl = new OverloadDispatcherClassLoader();

        // debug
        if (classDumpFile != null) {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(classDumpFile);
                fos.write(jc.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (fos != null)
                    try {
                        fos.close();
                    } catch (Exception e) {
                    }
            }
        }

        return cl.loadClassFromBytes(dispatcherClassName, jc.getBytes());

    }

    public static class Hierarchy implements Comparable<Hierarchy> {
        private List<Class<?>> classes = new ArrayList<Class<?>>();

        public Hierarchy(Class<?> clazz) {
            for (Class<?> currentClass = clazz; currentClass != null; currentClass = (Class) currentClass.getSuperclass()) {
                classes.add(currentClass);
            }
        }

        public Class<?> getTop() {
            if (classes.size() < 1) {
                return null;
            }
            return classes.get(0);
        }

        public List<Class<?>> getClasses() {
            return classes;
        }

        public boolean equals(Hierarchy other) {
            if (classes.size() != other.classes.size()) {
                return false;
            }

            for (int i = 0; i < classes.size(); i++) {
                if (!classes.get(i).equals(other.classes.get(i))) {
                    return false;
                }
            }

            return true;
        }

        public int compareTo(Hierarchy other) {
            if (this.equals(other)) {
                return 0;
            } else if (this.containsClass(other.getTop())) {
                return -1;
            }

            return 1;
        }

        private boolean containsClass(Class<?> other) {
            for (Class<?> clazz : classes) {
                if (clazz.equals(other)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class OverloadDispatcherClassLoader extends URLClassLoader {
        public OverloadDispatcherClassLoader() {
            super(new URL[] {});
        }

        public Class loadClassFromBytes(String name, byte[] bytes) {
            try {
                return this.loadClass(name);
            } catch (ClassNotFoundException cnfe) {
                // expected
            }
            return this.defineClass(name, bytes, 0, bytes.length);
        }
    }
}
