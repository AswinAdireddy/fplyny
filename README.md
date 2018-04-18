# Apache Spark and Python - pySpark CLI Tool

The CLI tool to build and query a random in-memory dataset of people with name, age, latitude and longitude between 10 million and 100 million entries.
Given a user's name and age, the tool provides a list of 10 people who are 'similar' and 'closest' within 1 second for at least 95% of queries.

------------------
Solution Overview:
------------------

    Part 1 : Dummy Data Source & Aggregation

    - Used Mockaroo Random Mock Data generator to generate 100+ files of 100,000 rows each.
    - The schema used was [name,lat,lon,age]
    - Then used the utility.mergeCsv() function to create a uber csv with 10,000,000 + entries
    - Sanitized the uber csv file to have only distinct values by removing any duplicates and NaN values.
      I used pandas library for the same.
    - I performed multiple trials with extreme boundary conditions to verify the realistic nature of the data.
        - For all the provided lat, lon entries in my trials, I was able to derive location for the provided coordinates.

    Part 1 & 2: In-Memory Data Ingestion , CLI App & Search

    - I have used Apache Spark, distributed computing engine for loading the data into memory to perform searches.
    - Used 'com.databricks.spark.csv' package to read and convert csv data into Spark DataFrame for in-memory computations.
    - After creating the necessary transformation on the Dataframe, query actions are performed.
    - For CLI, I have used python 'Click' library to create a simple interface for query.

-------------------------
Installation Instructions
-------------------------
    Pre-requisites:

        - Install Spark
        - Set the following env vars
            export SPARK_HOME='<INSTALLED_SPARK_HOME>'
            export PATH=$SPARK_HOME:$PATH
            export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

    Install:

        pip install --editable .

    Run:

    ./spark-submit ../fplyny.py
                   ../fplyny.py --help            # displays help message
                   ../fplyny.py --version         # displays script version information message
                   ../fplyny.py --proximity       # option to set proximity for proximity based searches. Default 1km
                   ../fplyny.py --view_proximity  # option to display and perform proximity based searches


------------------------------------
Find->People->Like->You->Near->You !
[FP-LY-NY]
------------------------------------

    This script demonstrates a simple app functionality where in a user can search people similar in the nearest
    vicinity given his/her age, latitude and longitude inputs.

    The app also supports a proximity search flow where given the aforementioned inputs, the user could iterate searches
    based on proximity distance in KiloMeter(KM).

    It has a simple administrative UI to display spark job statistics and execution environment details. It is accessible
    at the following url:

    Spark Job Interface : http://localhost:40404/jobs

    Application Flow Overview:

    [ StartNewSearch->GetUserInput->PerformSearch->DisplayResults
    ->ShowMoreDetails->DisplayResultsWithExactLocation
    ->PromptQuit->Quit(or)StartNewSearch ]

    With 'proximity' & 'view_proximity oprions set:

    [ StartNewSearch->GetUserInput->PromptProximity->PerformSearch->DisplayResults
    ->Show More Details->Display Results with Exact Location
    ->PromptQuit->Quit(or)Continue
    ->Proximity Search->Display Results with Proximity(or)StartNewSearch ]

    :Result View: Displays top 10 people in close proximity/location

----------
Unit Tests
----------
    - Tests can be found in the /tests/ directory.
    - I have used pytest to execute some basic test cases on queries used for the solution.

    run this code via:

    ../python -m pytest test_fplyny.py


------------------------
Dev Environment Overview
------------------------
    - VMWare Workstation 10
    - Ubuntu 16.04 LTS 64 bit
    - HDD 20G
    - RAM 16.8G
    - Python 2.6/2.7
    - Apache Spark 1.6.2/2.0.2 Pre-Built Binaries
        - Spark 1.6.2 & Hadoop 2.6
        - Spark 2.0.2 & Hadoop 2.7
    - PyCharm IDE

---------------------------
Observations / Known Issues
---------------------------
    - No Results : Dummy data generated schema [name: STRING, age: Int[10-116], lat: Float, lon: Float]
        - Hence all inputs for ages 0-10 should not return valid results.

    Workaround: Provide inputs for age between 10 - 116.

    - CLI Prompt Issue : It has been observed that during continual iterations with the app, one of the prompt
    disappears after successful result view.
        - Do you want to know the exact location of these people [y/n]  prompt disappears on certain searches.

    Workaround: On any key input the prompt is recovered. I have not been able to resolve this issue yet.

-------------------
Developer Notes
-------------------
    - All the observations are made by running Spark in 'local' mode.
    - In regard to the approaches for the challenge :
        pyspark.sql.SQLContext.sql query
        pyspark.sql.DataFrame API

        SQLContext is slow and consistent on multiple iterations DataFrame approach is fast and improves on multiple
        iterations. I did not observe any performance difference with various range of Spark Tuning parameters. Dataframe
        approach provided significant results on multiple iterations and could be capped at <1 sec except the first request.
        However in both the approaches there is a significant latency in deriving the location for the final approximated
        list of users.
