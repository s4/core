S4 Core Classes
===============

Introduction
------------
This is a component of the S4 streaming system. For more information, see [s4.io](http://s4.io)

Requirements
------------

* Linux
* Java 1.6
* Maven
* S4 Communication Layer

Build Instructions
------------------

1. First build and install the comm package in your Maven repository.

2. Kryo, Reflectasm, and minlog must be installed to your local Maven repository manually.
   The jars are present in lib/ within this project. To install, run the following commands:

        mvn install:install-file -DgroupId=com.esotericsoftware -DartifactId=kryo -Dversion=1.01 -Dpackaging=jar -Dfile=lib/kryo-1.01.jar
        mvn install:install-file -DgroupId=com.esotericsoftware -DartifactId=reflectasm -Dversion=0.8 -Dpackaging=jar -Dfile=lib/reflectasm-0.8.jar
        mvn install:install-file -DgroupId=com.esotericsoftware -DartifactId=minlog -Dversion=1.2 -Dpackaging=jar -Dfile=lib/minlog-1.2.jar

3. Build and install using Maven

        mvn assembly:assembly install

