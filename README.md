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


## Solutions 
Two solutions are created, 
 - Main.scala -- Uses extensive Spark Dataframe APIs alongside querying constructs / filters to arrive at solution.
 - ScalaAssignment.scala -- Focuses on extracting Dataframe, converting into Datasets using typesafe caseclasses and applying Scala functions to arrive at the solution


# Data 
- Two CSVs are provided with flight data and passenger data - flightData.csv, passengers.csv
 
