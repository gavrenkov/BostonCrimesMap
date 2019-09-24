# BostonCrimesMap

Homework for Data Engineer course from OTUS.

To run:

spark-submit --master local[*] --class com.example.BostonCrimesMap target/scala-2.11/BostonCrimesMap-assembly-1.0.jar data/crime.csv data/offense_codes.csv data/result
