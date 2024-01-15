#!/bin/bash
FIC=$1
hadoop com.sun.tools.javac.Main Select*.java
jar cf select.jar Select*.class
hadoop fs -rm -r -f /project/output*
hadoop jar select.jar Select /project/input$FIC /project/output
hadoop fs -cat /project/output/part-r-00000
