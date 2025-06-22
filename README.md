# Objective
Data Querying with Spark and Scala - explore Dataframes API and typesafe case classes in different ways.

## Queries 
1. Find the total number of flights for each month.
2. Find the names of the 100 most frequent flyers.
3. Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.
4. Find the passengers who have been on more than 3 flights together.
5. Find the passengers who have been on more than N flights together within the range (from,to).The function should look something like:
def flownTogether(atLeastNTimes: Int, from: Date, to: Date) = {
  ...
}


## About Solutions 
Two solutions are created, 
 - Main.scala -- Uses extensive Spark Dataframe APIs alongside querying using Spark constructs like groupBy, select and filters to arrive at solution.
 - ScalaAssignment.scala -- Focuses on extracting Dataframe, converting into Datasets using typesafe case classes and applying Scala functions to arrive at the solution

## Run Instructions
- Source CSV files are in the provided in the uploaded folders
- Execute program by running command : scala-cli ScalaAssignment.scala
- Note, above program needs Scala-Cli to be installed (Please refer to this link for install steps : https://scala-cli.virtuslab.org/install/) 
- Output files will be generated in the current folder
  - Outputs are generated as :
    - Question 1 : output-TotalFlightsByMonth
    - Question 2 : output-top100Flyers
    - Question 3 : output-LongestRun
    - Question 4 : output-FlownTogetherThrice
    - Question 5 : output-flownTogetherNTimes

# Data 
- Two CSVs - flightData.csv, passengers.csv
 
