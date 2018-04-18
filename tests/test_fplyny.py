"""
[FP-LY-NY] Unit Tests Module

Unit Tests for SPARK's API used in FPLYNY default flow

This module use pytest package for unit testing.

run this code via:
<python path> -m pytest test_fplyny.py

"""
import pytest

import os, sys,fplyny, utility
import constants as const
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf

# VARS

proxUDF = None
ulocUDF = None

un = fplyny.username
ua = fplyny.userage
ult = fplyny.userlat
uln = fplyny.userlon
up = fplyny.username

def quiet_logger(sparkContext):
    """
    Turn down spark logging for the test context

    :param sparkContext: Fixture

    :return: Logging OFF
    """
    logger = sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org.apache.spark").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("ivy").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("org.apache.hadoop").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("py4j").setLevel(logger.Level.OFF)


@pytest.fixture(scope="session")
def spark_context(request):
    """ Fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object

    Initialize Spark environment, load data into memory for each unit test execution.
    Returns the main data frame that test can use to perform search queries.
    """
    global proxUDF, ulocUDF

    # Setup Spark Env
    os.environ['SPARK_HOME'] = const.SP202_HOME_DIR
    os.environ['SPARK_SUBMIT_EXTRA_ARGS'] = const.SP_SUBMIT_ARGS
    os.environ['SPARK_DAEMON_MEMORY'] = const.SP_DAEMON_MEM
    os.environ['SPARK_LOCAL_IP'] = 'localhost'
    sys.path.append(const.PYSPARK202_HOME_DIR)

    # Initialize Spark Conifguration
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-fplyny-local-testing"))
    sc = SparkContext(conf=conf)

    # Turn Logging OFF
    quiet_logger(sc)

    request.addfinalizer(lambda: sc.stop())

    # Read Data into Memory
    hc = HiveContext(sc)
    testDataFrame = hc.read.format(const.DATABRICKS_SPARK_CSV).options(
        header='true', inferschema='true').load(const.FPLYNY_DATA_FILE)

    # Cache Data
    testDataFrame.persist(const.MEM_STORAGE_LEVEL)

    # Define Custom User Functions
    proxUDF = udf(calcD)
    ulocUDF = udf(calcL)

    return testDataFrame


def calcD(lt, ld):
    """
    Wrapper Method

    :return: proximity
    """
    import fplyny as app
    return app.calculateProximity(lt,ld)

def calcL(lt, ld):
    """
    Wrapper Method

    :return: location
    """
    import fplyny as app
    return app.calculateLocation(lt, ld)


def test_search_results_count(spark_context):
    """
    Test that result for user's age return only Top 10 items

    :return: TRUE
    """
    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    #lDF.rdd.flatMap(list).first()

    assert lDF.count() == 10


def test_search_results_first_row_element(spark_context):
    """
    Test that the first element of the results displayed is the expected value

    :param spark_context: Fixture
    :return: TRUE
    """
    expected = 'Daniel'

    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    assert lDF.rdd.flatMap(list).first() == expected

def test_search_results_rdd(spark_context):
    """
    Test that results returned in the search are as expected

    :param spark_context: Fixture
    :return: TRUE
    """
    expected = [(u'Daniel', 10, 18.1011, -96.797),
               (u'Carl', 10, 19.2707, -97.1862),
               (u'Ronald', 10, 19.3347, -96.6206),
               (u'Scott', 10, 19.1508, -96.3081),
               (u'Kathy', 10, 17.9036, -94.9456),
               (u'Jack', 10, 18.7149, -99.9151),
               (u'Emily', 10, 19.0992, -99.6565),
               (u'Eric', 10, 19.2619, -99.7098),
               (u'Helen', 10, 19.7052, -99.4323),
               (u'Donna', 10, 19.2958, -101.9675)]

    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    assert lDF.rdd.collect() == expected

def test_search_results_loc_rdd(spark_context):
    """
    Test the final results set shown has location and is as expected.

    :param spark_context: Fixture
    :return:
    """
    expected = [(u'Daniel', 10, u'MEX 182, Los Pinos, Huautepec, Oaxaca, M\xe9xico'),
                (u'Carl', 10, u'Rafael J. Garc\xeda, Chilchotla, Puebla, M\xe9xico'),
                (u'Ronald', 10, u'Carrizal - Xotla, Xotlilla, Tlaltetela, Veracruz de Ignacio de la Llave, M\xe9xico'),
                (u'Scott', 10,
                 u'Avenida 2 de Abril, Manlio Fabio Altamirano, Veracruz de Ignacio de la Llave, M\xe9xico'),
                (u'Kathy', 10,
                 u'MEX 185, Sayula de Aleman, Sayula de Alem\xe1n, Veracruz de Ignacio de la Llave, 96165, M\xe9xico'),
                (u'Jack', 10, u'Las Joyas, Sultepec, M\xe9xico'),
                (u'Emily', 10, u'Miguel Hidalgo, Acatzingo, Tenango del Valle, M\xe9xico'),
                (u'Eric', 10, u'Calle Industria Pte., Cacalomacan, Toluca, M\xe9xico, 1525, M\xe9xico'),
                (u'Helen', 10, u'Carretera a San Jeronimo Zacapexco, Villa del Carb\xf3n, M\xe9xico'),
                (u'Donna', 10,
                 u'Carretera Municipal: Taretan - Emiliano Zapata - San Marcos, Taretan, Michoac\xe1n de Ocampo, M\xe9xico')]

    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    dDF = lDF.withColumn("location", ulocUDF(lDF.latitude, lDF.longitude)).select("name", "age", "location")

    assert dDF.rdd.collect() == expected
    assert dDF.count() == 10


def test_incorrect_search_results_count(spark_context):
    """
    Negative Test that result for user's age return only Top 10 items

    :return: TRUE
    """
    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    #lDF.rdd.flatMap(list).first()

    assert lDF.count() != 9

def test_incorrect_search_results_first_row_element(spark_context):
    """
    Negative Test that the first element of the results displayed is not the expected value

    :param spark_context: Fixture
    :return: TRUE
    """
    not_expected = 'Sarah'

    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    assert lDF.rdd.flatMap(list).first() != not_expected

def test_incorrect_search_results_rdd(spark_context):
    """
    Test that results returned in the search are as expected

    :param spark_context: Fixture
    :return: TRUE
    """
    not_expected = [(u'Daniel', 11, 18.1011, -96.797),
               (u'Carl', 11, 19.2707, -97.1862),
               (u'Ronald', 11, 19.3347, -96.6206),
               (u'Scott', 11, 19.1508, -96.3081),
               (u'Kathy', 11, 17.9036, -94.9456),
               (u'Jack', 11, 18.7149, -99.9151),
               (u'Emily', 11, 19.0992, -99.6565),
               (u'Eric', 11, 19.2619, -99.7098),
               (u'Helen', 11, 19.7052, -99.4323),
               (u'Donna', 11, 19.2958, -101.9675)]

    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    assert lDF.rdd.collect() != not_expected

def test_search_results_loc_rdd(spark_context):
    """
    Test the final results set shown has location and is as expected.

    :param spark_context: Fixture
    :return:
    """
    not_expected = [(u'Daniel', 11, 18.1011, -96.797),
                (u'Carl', 11, 19.2707, -97.1862),
                (u'Ronald', 11, 19.3347, -96.6206),
                (u'Scott', 11, 19.1508, -96.3081),
                (u'Kathy', 11, 17.9036, -94.9456),
                (u'Jack', 11, 18.7149, -99.9151),
                (u'Emily', 11, 19.0992, -99.6565),
                (u'Eric', 11, 19.2619, -99.7098),
                (u'Helen', 11, 19.7052, -99.4323),
                (u'Donna', 11, 19.2958, -101.9675)]

    import pyspark.sql.functions as f

    pDF = spark_context.filter(spark_context.age == ua).withColumn(
        "proximity_in_km", proxUDF(spark_context.latitude, spark_context.longitude))
    # cache for proximity searches
    pDF.persist(const.StorageLevel.MEMORY_ONLY_SER)
    lDF = pDF.sort(
        f.col("proximity_in_km"), ascending=True).limit(10).select(
        "name", "age", "latitude", "longitude")

    dDF = lDF.withColumn("location", ulocUDF(lDF.latitude, lDF.longitude)).select("name", "age", "location")

    assert dDF.rdd.collect() != not_expected
    assert dDF.count() == 10