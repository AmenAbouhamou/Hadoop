#!/bin/bash
FIC=$1
pattern="[a-zA-Z]+[0-9]+?\.csv" #pattern fichier.csv
pattern2="[all]" # pattern pour selectionner tous les fichiers

hadoop com.sun.tools.javac.Main Select*.java
jar cf select.jar Select*.class
hadoop fs -rm -r -f /project/output*

if [[ "$FIC" =~ $pattern ]]; then
  hadoop jar select.jar Select /project/input/$FIC /project/output
elif [[ "$FIC" =~ $pattern2 ]]; then
  hadoop jar select.jar Select /project/input /project/output
fi

hadoop fs -cat /project/output/part-r-00000
