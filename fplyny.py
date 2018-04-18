#!/usr/bin/env/python
"""

Find->People->Like->You->Near->You ! [FP-LY-NY]

This script demonstrates a simple app functionality where in a user can search people similar in the nearest
vicinity given his/her age, latitude and longitude inputs.

The app also supports a proximity search flow where given the aforementioned inputs, the user could iterate searches
based on proximity distance in KiloMeter(KM).

This script is pySpark application and uses Apache Spark 1.6.2 / 2.0.2, a distributed computing engine and a bunch of
python libraries like click, databricks csv, numpy, pandas, pygeocoder among the few.


run this code via:
   spark-submit ../fplyny.py
                ../fplyny.py --help            # displays help message
                ../fplyny.py --version         # displays script version information message
                ../fplyny.py --proximity       # option to set proximity for proximity based searches. Default 1km
                ../fplyny.py --view_proximity  # option to display and perform proximity based searches

"""
import os, sys, click
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import HiveContext, SQLContext, SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as f
from click import Abort
import constants as const
import utility as util
from time import time

# Variables #
username = 'anon'
userage = 10
userlat = 16.7033
userlon = -97.6617
userproximity = 1.0 # 1000 meters
retryopts = False
dataFrame = StructType([])
proxDataFrame = StructType([])
sparkConf = None
sparkContext = None
sqlContext = None
hiveContext = None
viewProximity = False # flag to view proximity column in results

# EoVs #

# Solutions - Toggle
# 2 Approaches:
#  pyspark.sql.SQLContext - Main entry point for DataFrame and SQL functionality.
#  pyspark.sql.DataFrame API - A distributed collection of data grouped into named columns.
#
#
#
sparkSqlApproach = False

# FUNCTIONS #

def resetVars():
    """
    TearDown
    :return: resets all user input variables
    """
    global username, userage, userlat, userlon, userproximity, retryopts
    username = ''
    userage = 10
    userlat = 0.0
    userlon = 0.0
    userproximity = 0.5
    retryopts = False

def getUserInput():
    """
    Get required user inputs - username, user age, latitude, longitude
    :return: users approximate geo-location
    """
    global username, userage, userlat, userlon
    while True:
        try:
            username = click.prompt('[FP-LY-NY] Please enter your first name', type=str)
            username = username.encode('ascii', 'ignore')
        except ValueError:
            click.echo('[FP-LY-NY] [ERR] That\'s not a name! Try again')
            continue
        else:
            if str.isalpha(username):
                break
            else:
                click.echo('[FP-LY-NY] [ERR] That\'s not a valid name! Try again')
                continue

    while True:
        try:
            userage = click.prompt('[FP-LY-NY] Please enter your age', type=click.IntRange(0, 116))
        except ValueError:  # catch the exceptions you know!
            click.echo('[FP-LY-NY] [ERR] That\'s not a number! Try again')
            continue
        else:
            break

    while True:
        try:
            userlat = click.prompt('[FP-LY-NY] Please enter your location details in latitude', type=click.FLOAT)
        except ValueError:  # catch the exceptions you know!
            click.echo(
                '[FP-LY-NY] [ERR] That\'s not a valid latitude! Try again')
            continue
        else:
            break

    while True:
        try:
            userlon = click.prompt(
                '[FP-LY-NY] Please enter your location details in longitude', type=click.FLOAT)
        except ValueError:  # catch the exceptions you know!
            click.echo('[FP-LY-NY] [ERR] That\'s not a valid longitude! Try again')
            continue
        else:
            break

def getProximityPrompt():
    """
    Get proximity of search if OPTION is entered
    :return: new proximity for proximity based searches
    """
    global userproximity
    while True:
        try:
            num = click.prompt("[FP-LY-NY] Please enter new proximity in Kilo Meter for search :", type=click.FLOAT)
        except ValueError:  # catch the exceptions you know!
            print ("[FP-LY-NY] [ERR] That\'s not  valid proximity input! Try again")
            continue
        else:
            userproximity = num
            break

def continueOpts():
    """
    Get Proximity Inputs if user wishes to retry during the flow with proximity ranges
    :return: new search or proximity search
    """
    global retryopts
    while True:
        userprompt = click.prompt(
            '[FP-LY-NY] Do you want to search with new proximity value or start a new search [p/n] :', type=click.Choice(['p', 'n']))
        if userprompt == "p":
            getProximityPrompt()
            break
        else:
            retryopts = False
            break


def quitPrompt():
    """
    Options while terminating App function. User can choose to continue with new searches.
    :return: set flag for new search or quit app
    """
    while True:
        global retryopts
        try:
            print ('\n')
            prompt = click.prompt('[FP-LY-NY] Do you want to continue or quit [c/q]', type=click.Choice(['c', 'q']))
        except ValueError:
            print '[FP-LY-NY] [ERR] That\'s not a valid input! Try again'
            continue
        else:
            if prompt == "c":
                print ("\n")
                print ("[FP-LY-NY] Great! let's continue to do some more searches...")
                print ("\n")
                retryopts = True
                break
            else:
                retryopts = False
                break

def quit():
    """
    Terminate App function
    :return: Confirmation check before quiting.
    """
    try:
        click.confirm(
            '[FP-LY-NY] Are you sure you want to quit this App?', default=False, abort=True, prompt_suffix=': ', show_default=True, err=True)
    except Abort:
        print ('\n')
        print '[FP-LY-NY] Starting a new search ...'
        print ('\n')
        pass
    else:
        if sparkSqlApproach:
            shutdownSparkSession()
        print ("\n")
        print (const.THANK_YOU_MSG)
        sys.exit(0)

def bootstrap():
    """
    Set Spark Environment
    :return: system variables set for spark environment
    """
    os.environ['SPARK_HOME'] = const.SP202_HOME_DIR
    os.environ['SPARK_SUBMIT_EXTRA_ARGS'] = const.SP_SUBMIT_ARGS
    os.environ['SPARK_DAEMON_MEMORY'] = const.SP_DAEMON_MEM
    os.environ['SPARK_LOCAL_IP'] = 'localhost'

    sys.path.append(const.PYSPARK202_HOME_DIR)

def initSpark():
    """
    Initialize Spark 1.6.2 / 2.0.2 Configuration and Spark, SQL & Hive Context

    :return: Spark Session
    """
    global sparkConf, sparkContext, sqlContext, hiveContext
    t0 = time()
    # Set env and config params
    bootstrap()
    # Initialize Spark
    sparkConf = (SparkConf()
                .set("spark.master", const.MASTER)
                .set("spark.app.name", const.APP_NAME)
                .set("spark.driver.memory", const.SPARK_DRIVER_MEMORY)
                .set("spark.executor.memory", const.SPARK_EXECUTOR_MEMORY)
                .set("spark.executor.instances", const.SPARK_EXECUTOR_INSTANCES)
                .set("spark.executor.cores", const.SPARK_EXECUTOR_CORES)
                .set("spark.ui.port", "40404") # Spark Admin UI
                .set("spark.serializer", const.SPARK_SERIALIZER))
                #.set("spark.cores.max", "10")
                #.set("spark.dynamicAllocation.enabled", const.SPARK_DYNAMIC_ALLOCATION_ENABLE)
                #.set("spark.shuffle.service.enabled", const.SPARK_SHUFFLE_SRVC_ENABLE)
                # .set("eventLog.enabled", "true")
                # .set("spark.logConf", "true")

    # Get Spar Context
    sparkContext = SparkContext(conf=sparkConf)

    sparkContext.setLogLevel("OFF")

    # logger.LogManager.getLogger("org.apache.spark").setLevel(logger.Level.INFO)

    # Disable console logging
    logger = sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org.apache.spark").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("ivy").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("org.apache.hadoop").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("py4j").setLevel(logger.Level.OFF)

    sqlContext = SQLContext(sparkContext)
    hiveContext = HiveContext(sparkContext)

    # Load data in memory
    loadDataInMemory()
    tt = time() - t0
    print "[FP-LY-NY] Loaded data in memory in {} seconds".format(round(tt, 3))


def shutdownSparkSession():
    """
    Shutdown Spark Session on exit.
    :return: stop spark session
    """



def loadDataInMemory():
    """
    Load CSV data into data frames
    Using com.databricks.spark.csv package for parsing and querying CSV data into dataframes.
    The data frame is then sanitized to check for
    :return: the main dataframe that user performs proximity searches
    """
    global dataFrame

    dataFrame = hiveContext.read.format(const.DATABRICKS_SPARK_CSV).options(
        header='true', inferschema='true').load(const.FPLYNY_DATA_FILE)

    # Cache the main data frame for filtering
    dataFrame.persist(const.MEM_STORAGE_LEVEL)
    # dataFrame.cache() # StorageLevel.MEMORY_ONLY
    #dataFrame.persist(StorageLevel.MEMORY_ONLY_SER)   #persisting the data as a serialized byte array


def calculateProximity(latd, lond):
    """
    Wrapper for proximity utility method
    User Defined Function for Spark DataFrame Operations
    The first 2 coordinates userlon, userlat are user provided input values

    :param latd: latitude in a particular row of dataframe
    :param lond: longitude in a particular row of dataframe

    :return: relative distance between the 4 provided coordinates
    """
    return util.determineDistance(userlat, userlon, latd, lond)


def calculateLocation(latd, lond):
    """
    Wrapper for user location utility
    User Defined Function for Spark DataFrame Operations

    :param latd: latitude in a particular row of dataframe
    :param lond: longitude in a particular row of dataframe

    :return: Approximate city location
    """
    return util.getGeoLocation(latd, lond)

def searchUsingSQL():
    """
    TESTING ONLY
    use flag 'sqlQueryApproach = True' for approach toggle.

    Using sqlCtxt - Can we return in 1 query. Location is taking time.
    Issue - Cant derive location for entire dataset.

    :return: Display Search Results
    """

    t0 = time()

    # Register the main dataframe as a table
    sqlContext.registerDataFrameAsTable(dataFrame, "people")
    # Register 3 functions for use in the query. Both SQLExp and DSL API functions
    sqlContext.registerFunction("calculateDistance", lambda x, y: calculateProximity(x, y))
    sqlContext.registerFunction("calculatePlace", lambda x, y: calculateLocation(x, y))
    user_location_udf = udf(calculateLocation)

    # Cache the table for further processing
    sqlContext.cacheTable("people")

    if viewProximity:
        # This query will block your account due to the restrictions geo-location API requests.
        # query = 'SELECT name, age, calculateDistance(latitude, longitude) AS proximity,' \
        #         'calculatePlace(latitude, longitude) AS location ' \
        #         'FROM people WHERE age = {} AND proximity <= {}' \
        #         'ORDER BY proximity ASC'.format(userage, userproximity)

        # Selects requisite columns on the criteria age = <user-age> & proximity <= <user-proximity>
        # Bug in logic - cant evaluate proxmity in this query logic fails for smaller sets.
        # Would need another query ....?
        query = 'SELECT name, age, latitude, longitude, calculateDistance(latitude, longitude) AS proximity ' \
                'FROM people ' \
                'WHERE age = {} ' \
                'ORDER BY proximity ASC'.format(userage, userproximity) #{:.9f}
    else:
        # query = 'SELECT name, age, calculatePlace(latitude, longitude) AS location FROM people ' \
        #             'WHERE age = {} AND calculateDistance(latitude, longitude) <= {}'.format(userage, userproximity)

        # query = 'SELECT name, age, latitude, longitude, calculateDistance(latitude, longitude) AS proximity ' \
        #     'FROM people ' \
        #     'WHERE age = {} ' \
        #     'ORDER BY proximity ASC'.format(userage)

        # query = 'SELECT name, age, latitude, longitude, calculateDistance(latitude, longitude) AS proximity ' \
        #     'FROM people ' \
        #     'WHERE age = {} ' \
        #     'ORDER BY proximity ASC'.format(userage)

        query = 'SELECT name, age, latitude, longitude, calculateDistance(latitude, longitude) AS proximity ' \
                'FROM people ' \
                'WHERE age = {} ' \
                'ORDER BY proximity ASC'.format(userage, userproximity)

    print '\n'
    print ("--------------------------------------------------------")
    print (const.RESULT_BANNER)
    print ("--------------------------------------------------------")

    # Execute query and cap the dataframe to top 10 records
    proxDFrame = sqlContext.sql(query).limit(10)

    if not viewProximity:
        t1 = time()
        proxDFrame = proxDFrame.select(
                    "name", "age", "latitude", "longitude")
        tt = time() - t1
        print "LatLonSortedDF in {} seconds".format(round(tt, 3))

    # Persist this dataframe
    proxDFrame.persist(const.StorageLevel.MEMORY_ONLY_SER)

    # Display results - [name, age, lat, lon, [proximity]]
    proxDFrame.show()

    tt = time() - t0
    print "ProximitySortedDF in {} seconds".format(round(tt, 3))

    # Show More Details Prompt
    # Choose to see the exact location of the top ten people
    # Deriving location is done using the geopy API.
    # On observations, it has been noted that this task is relatively slow compared to deriving people in
    # close proximity.
    while True:
        try:
            showMorePrompt = click.prompt(
                '[FP-LY-NY] Do you want to know the exact location of these people [y/n] :',
                type=click.Choice(['y', 'n']))
            # tt = time() - t0
            # print "Location DF in {} seconds".format(round(tt, 3))

        except ValueError:  # catch the exceptions you know!
            click.echo('[FP-LY-NY] [ERR] That\'s not a valid input! Try again')
            continue
        else:
            if showMorePrompt == "n":
                break # get out of the loop, let quit-prompt handle next input
            t0 = time()
            # Now that we have our top 10 ascended people dataframe, we call an
            # user defined function 'calculatePlace' that uses geopy API to
            # derive the address corresponding to the coordinates.
            # We are simply adding an another column with this derived value and
            # creating our final view [name, age, location] or [name, age, location, [proximity]]
            if viewProximity:
                proxDFrame.withColumn(
                    "location", user_location_udf(
                        proxDFrame.latitude, proxDFrame.longitude)).select(
                    "name", "age", "location", "proximity").show()
            else:
                proxDFrame.withColumn(
                    "location", user_location_udf(
                        proxDFrame.latitude, proxDFrame.longitude)).select(
                    "name", "age", "location").show()
            tt = time() - t0
            print "Result view in {} seconds".format(round(tt, 3))
            break # we are done, let quit-prompt handle next input


def searchForRelatedPeople():
    """
    Performs Spark Filter, Select and Display functions
    First filter dataframe based on age criteria and derive proximity for those criteria matched results.
    Cache this for proximity searches
    :return: Display Search Results
    """
    global proxDataFrame
    user_proximity_udf = udf(calculateProximity)
    user_location_udf = udf(calculateLocation)

    proxDataFrame = dataFrame.filter(dataFrame.age == userage).withColumn(
        "proximity_in_km", user_proximity_udf(dataFrame.latitude, dataFrame.longitude))
    # cache for proximity searches
    proxDataFrame.persist(const.StorageLevel.MEMORY_ONLY_SER) #cache()

    print '\n'

    print (const.RESULT_BANNER)


    t0 = time()
    if viewProximity:
        locationDataFrame = proxDataFrame.filter(
            proxDataFrame.proximity_in_km <= userproximity).sort(
            f.col("proximity_in_km"), ascending=True).select(
                "name", "age", "latitude", "longitude", "proximity_in_km").limit(10)
    else:
        locationDataFrame = proxDataFrame.sort(
            f.col("proximity_in_km"), ascending=True).limit(10).select(
                "name", "age", "latitude", "longitude")
        locationDataFrame.persist(const.StorageLevel.MEMORY_ONLY_SER)   #cache()

    locationDataFrame.show()

    tt = time() - t0
    print "[FP-LY-NY] Final Dataframe in {} seconds".format(round(tt, 3))

    # Show More Details Prompt
    # Choose to see the exact location of the top ten people
    # Deriving location is done using the geopy API.
    # On observations, it has been noted that this task is relatively slow compared to deriving people in
    # close proximity.
    while True:
        try:
            showMorePrompt = click.prompt(
                '[FP-LY-NY] Do you want to know the exact location of these people [y/n] :',
                type=click.Choice(['y', 'n']))
            # tt = time() - t0
            # print "Location DF in {} seconds".format(round(tt, 3))

        except ValueError:  # catch the exceptions you know!
            click.echo('[FP-LY-NY] [ERR] That\'s not a valid input! Try again')
            continue
        else:
            if showMorePrompt == "n":
                break
            print ("\n")

            print (const.LOC_RESULT_BANNER)


            # Retrieve Coordinate List from the dataframe for independent query of geo location
            # t0 = time()
            # # New code
            # #mylist = locationDataFrame.selectExpr("latitude as lat", "longitude as lon").collect()
            # mylist = locationDataFrame.select("latitude", "longitude"). collect()
            # print mylist
            # tt = time() - t0
            # print "Coordinate List in {} seconds".format(round(tt, 3))

            t0 = time()
            # t0 = time()
            if viewProximity:
                ddf = locationDataFrame.withColumn(
                    "location", user_location_udf(
                        locationDataFrame.latitude, locationDataFrame.longitude)).select(
                    "name", "age", "location", "proximity_in_km")
            else:
                ddf = locationDataFrame.withColumn(
                    "location", user_location_udf(
                        locationDataFrame.latitude, locationDataFrame.longitude)).select(
                    "name", "age", "location")
            ddf.show()
            tt = time() - t0
            print "[FP-LY-NY] Location Dataframe in {} seconds".format(round(tt, 3))
            break # We are done showing extra details


def flushPrimaryCache():
    """ Flushes Age DataFrame Cache for new searches"""
    dataFrame.unpersist()

def flushSecondaryCache():
    """ Flushes Age DataFrame Cache for new searches"""
    if sparkSqlApproach:
        print 'Uncaching People Table'
        sqlContext.uncacheTable("people")
        sqlContext.dropTempTable("people")
    else:
        proxDataFrame.unpersist(blocking=True)

# EoFN()s #

# Click library for  Command Line Interface #

def print_version(ctx, param, value):
    """ App version info"""
    if not value or ctx.resilient_parsing:
        return
    click.echo('-------------------------------------------')
    click.echo('Find People You Like & Near You Application')
    click.echo('             [FP-LY-NY]                    ')
    click.echo('            Version  1.0                   ')
    click.echo('-------------------------------------------')
    ctx.exit()

@click.command()
@click.option(
    '--version', help='Displays application version info\n',
    is_flag=True, callback=print_version,
    expose_value=False, is_eager=True
)
@click.option(
    '--proximity', help='Distance in KM defining the search radius \n '
                        '(optional) \n '
                        'depedency: view_proximity option\n',
    is_flag=True, type=click.FLOAT
)
@click.option(
    '--view_proximity', help='Display Proximity in KM in the results \n '
                             '(optional) \n '
                             'depedency: proximity option \n'
                             'Result Views: \n'
                             '- [name, age, lat, lon]\n'
                             '- [name, age, lat, lon, proximity]\n'
                             '- [name, age, location]\n '
                             '- [name, age, location, proximity]\n ',
    is_flag=True, type=click.FLOAT
)

def cli(proximity, view_proximity):
    """
    Find->People->Like->You->Near->You ! [FP-LY-NY]\n
    -----------------------------------------------\n

    A simple app where a user can search people similar to his/her age in the nearest
    vicinity given his/her age, latitude and longitude inputs.

    The app also supports a proximity search flow where given the aforementioned inputs, user could iterate searches based on
    proximity distance in KiloMeter(KM). It has a simple administrative UI to display spark job statistics and execution
    environment details.

    This script is a pySpark application and uses Apache Spark 1.6.2 / 2.0.2, a distributed computing engine and a bunch of
    python libraries like click, databricks csv, numpy, pandas, pygeocoder among the few.


    run this code via:\n
    ./spark-submit fplyny.py


    Application Flow Overview:

    [ StartNewSearch->GetUserInput->PerformSearch->DisplayResults
    ->ShowMoreDetails->DisplayResultsWithExactLocation
    ->PromptQuit->Quit(or)StartNewSearch ]

    With 'proximity' & 'view_proximity oprions set:

    [ StartNewSearch->GetUserInput->PromptProximity->PerformSearch->DisplayResults
    ->Show More Details->Display Results with Exact Location
    ->PromptQuit->Quit(or)Continue
    ->Proximity Search->Display Results with Proximity(or)StartNewSearch ]

    :return: displays top 10 people in close proximity/location

    """
    global viewProximity
    global retryopts

    print ("----------------------------+++++++++++++++----------------------------")
    click.echo(const.WELCOME_MSG)
    print ("----------------------------+++++++++++++++----------------------------")

    # Initialize Spark
    initSpark()

    ### FP-LM-NM APP ###
    while True:
        if not retryopts:
            # Initialize
            resetVars()
            if proximity and view_proximity:
                click.echo("[FP-LY-NY] View Proximity Info Option is Set to ON")
                viewProximity = True

            # Get required user inputs : username | userage | latitude | longitude
            getUserInput()
            # Only when option is set
            if viewProximity:
                getProximityPrompt()
                click.echo("[FP-LY-NY] Proximity Radius is now set at {} km".format(userproximity))
            click.echo('\n')
            try:
                user_location = util.getGeoLocation(userlat, userlon).encode('utf-8')
            except StandardError:
                click.echo("[FP-LY-NY] Your current approximate location is - Unknown")
            else:
                click.echo("[FP-LY-NY] Your current approximate location is - {}".format(user_location))

            if viewProximity:
                click.echo("[FP-LY-NY]  Locating people similar to you in a {} km radius.".format(userproximity))


        if sparkSqlApproach:
            # SQL Query Approach
            searchUsingSQL()
        else:
            # Dataframe Query Approach
            searchForRelatedPeople()

        quitPrompt()
        if retryopts:
            if viewProximity:
                continueOpts()
            else:
                retryopts = False
        else:
            flushPrimaryCache() # main csv dataframe
            quit()
        if not retryopts:
            """"""
            # similar age and proximity calculated data frame, for proximity search retries
            flushSecondaryCache()

if __name__ == '__main__':
    cli()