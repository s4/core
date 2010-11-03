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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Random;

import org.apache.bcel.Constants;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.BranchInstruction;
import org.apache.bcel.generic.ClassGen;
import org.apache.bcel.generic.ConstantPoolGen;
import org.apache.bcel.generic.INSTANCEOF;
import org.apache.bcel.generic.InstructionConstants;
import org.apache.bcel.generic.InstructionFactory;
import org.apache.bcel.generic.InstructionHandle;
import org.apache.bcel.generic.InstructionList;
import org.apache.bcel.generic.MethodGen;
import org.apache.bcel.generic.ObjectType;
import org.apache.bcel.generic.PUSH;
import org.apache.bcel.generic.Type;

public class ClonerGenerator {
    public Class generate(Class clazz) {
        String className = clazz.getName();
        Random rand = new Random(System.currentTimeMillis());
        String clonerClassname = "Cloner" + (Math.abs(rand.nextInt() % 3256));

        ClassGen cg = new ClassGen(clonerClassname,
                                   "java.lang.Object",
                                   clonerClassname + ".java",
                                   Constants.ACC_PUBLIC | Constants.ACC_SUPER,
                                   new String[] { "io.s4.util.Cloner" });
        ConstantPoolGen cp = cg.getConstantPool();
        InstructionFactory instFactory = new InstructionFactory(cg, cp);

        InstructionList il = new InstructionList();

        // build constructor method for new class
        MethodGen constructor = new MethodGen(Constants.ACC_PUBLIC,
                                              Type.VOID,
                                              Type.NO_ARGS,
                                              new String[] {},
                                              "<init>",
                                              clonerClassname,
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

        // build clone method
        il = new InstructionList();
        MethodGen method = new MethodGen(Constants.ACC_PUBLIC,
                                         Type.OBJECT,
                                         new Type[] { Type.OBJECT },
                                         new String[] { "arg0" },
                                         "clone",
                                         clonerClassname,
                                         il,
                                         cp);

        il.append(InstructionConstants.ACONST_NULL);
        il.append(InstructionFactory.createStore(Type.OBJECT, 2));
        il.append(InstructionFactory.createLoad(Type.OBJECT, 1));
        il.append(new INSTANCEOF(cp.addClass(new ObjectType(className))));
        BranchInstruction ifeq_6 = InstructionFactory.createBranchInstruction(Constants.IFEQ,
                                                                              null);
        il.append(ifeq_6);
        il.append(InstructionFactory.createLoad(Type.OBJECT, 1));
        il.append(instFactory.createCheckCast(new ObjectType(className)));
        il.append(InstructionFactory.createStore(Type.OBJECT, 2));
        InstructionHandle ih_14 = il.append(InstructionFactory.createLoad(Type.OBJECT,
                                                                          2));
        il.append(new INSTANCEOF(cp.addClass(new ObjectType("java.lang.Cloneable"))));
        BranchInstruction ifne_18 = InstructionFactory.createBranchInstruction(Constants.IFNE,
                                                                               null);
        il.append(ifne_18);
        il.append(instFactory.createFieldAccess("java.lang.System",
                                                "out",
                                                new ObjectType("java.io.PrintStream"),
                                                Constants.GETSTATIC));
        il.append(new PUSH(cp, "Not cloneable!"));
        il.append(instFactory.createInvoke("java.io.PrintStream",
                                           "println",
                                           Type.VOID,
                                           new Type[] { Type.STRING },
                                           Constants.INVOKEVIRTUAL));
        il.append(InstructionConstants.ACONST_NULL);
        il.append(InstructionFactory.createReturn(Type.OBJECT));
        InstructionHandle ih_31 = il.append(InstructionFactory.createLoad(Type.OBJECT,
                                                                          2));
        il.append(instFactory.createInvoke(className,
                                           "clone",
                                           Type.OBJECT,
                                           Type.NO_ARGS,
                                           Constants.INVOKEVIRTUAL));
        il.append(InstructionFactory.createReturn(Type.OBJECT));
        ifeq_6.setTarget(ih_14);
        ifne_18.setTarget(ih_31);
        method.setMaxStack();
        method.setMaxLocals();
        cg.addMethod(method.getMethod());
        il.dispose();

        JavaClass jc = cg.getJavaClass();
        ClonerClassLoader cl = new ClonerClassLoader();

        return cl.loadClassFromBytes(clonerClassname, jc.getBytes());
    }

    public static class ClonerClassLoader extends URLClassLoader {
        public ClonerClassLoader() {
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
