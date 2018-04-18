#!/usr/bin/env/python

"""
[FP-LY-NY] Utility Module

This script provides utility methods from deriving user location given his coordinates, to perform math functions to
derive relative distance given the latitude and longitude coordinates and a bunch of csv functions for creating the
dataset for Spark.

The data files are generated from http://mockaroo.com.
Over 100 files with 100,000 rows of data which is sanitized and regenerated to provide a near realistic In-Memory store
for query & search using Spark.

Data files can be found in fplyny/data/ directory

"""
import pandas, glob, os, csv
import numpy as np
import constants as const
from pygeocoder import Geocoder as geo
from pygeocoder import GeocoderError
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderQueryError
from geopy.distance import vincenty, great_circle
from math import sin, pi, acos, ceil, sqrt, cos, asin, radians


def getGeoLocation(latd, lond):
    """
    Determine address corresponding to a set of coordinates

    :param latd: coordinate1
    :param lond: coordinate2
    :return: geo address string
    """
    geolocator = Nominatim()
    geoargs = "{},{}".format(latd, lond)

    try:
        location = geolocator.reverse(geoargs)
    except (StandardError, GeocoderQueryError):
        #print ("[FP-LY-NY] [ERR] Invalid Coordinates. Cannot be mapped")
        return 'UnKnown'
    else:
        return location.address

def getUserLocation(latd, lond):
    """
    Get users location given the coordinates
    Uses pygeocoder, a python interface for Google Geocoding API V3
    Given the lat, lon it reverse geocodes location

    ToDO - Still returns null in the dataset
    need a way to handle it

    BLOCKED!!!

    """
    print latd, lond
    if latd != None and lond != None:
        try:
            location = geo.reverse_geocode(latd, lond)
        except GeocoderError:  # catch the exceptions you know!
            print '***********************************************'
            return 'Patagonia'
        except UnicodeEncodeError:
            print '***********************************************'
            return 'Patagonia'
        else:
            if location != None:
                return location.city
            else:
                return 'Patagonia'
    else:
        return 'Patagonia'

def getGeoDistance(lat, lon, latd, lond):
    """
    Geopy can calculate geodesic distance between two points using the Vincenty distance or great-circle
    distance formulas, with a default of Vincenty available as the class geopy.distance.distance,
    and the computed distance available as attributes

    :param lat: coordinate1
    :param lon: coordinate2
    :param latd: coordinate3
    :param lond: coordinate4
    :return: miles, meters , etx
    """
    x = (lat,lon)
    y = (latd,lond)

    print vincenty(x, y)
    print great_circle(x,y)

    return vincenty(x,y)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    :param lat: coordinate1
    :param lon: coordinate2
    :param latd: coordinate3
    :param lond: coordinate4
    :return: miles, meters , etx
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    y = c * r

    print y

    return y

def determineDistance(lat, lon, latd, lond):
    """
    Computing distance using lat/long coordinates
    Convert latitude and longitude to spherical coordinates.
    Assume the radius of the earth is 3960 miles (6373 kilometers) i.e. p = 3960
    'Theta' is the angle from the north pole down to the geographical location and 'Phi'
    is the angle from the north pole down to the geographical location. Given two points
    in spherical coordinates , the arc is derived by connecting the points
    """
    # Convert latitude and longitude to
    # spherical coordinates in radians.
    degrees_to_radians = pi / 180.0
    latd = np.float32(latd)
    lond = np.float32(lond)
    # phi = 90 - latitude
    phi1 = (90.0 - lat) * degrees_to_radians
    phi2 = (90.0 - latd) * degrees_to_radians
    # theta = longitude
    theta1 = lon * degrees_to_radians
    theta2 = lond * degrees_to_radians
    # Compute spherical distance from spherical coordinates.
    cosvalue = (sin(phi1) * sin(phi2) * cos(theta1 - theta2) +
                cos(phi1) * cos(phi2))
    arc = ceil((acos(cosvalue)) * 100) / 100
    #print arc

    return arc


def mergeCsv(indir = const.MRG_IN_DIR, outfile = const.MRG_OUT_FILE):
    """
    Merge multiple csv files into one uber csv file

    Data Files bundled in ~/data folder
    Files are merged to create a uber file.
    Uber file is sanitized for NaN values

    """

    os.chdir(indir)
    cfilelist = glob.glob("*.csv")

    if not os.path.exists(outfile):
        # empty csv list to hold all the files in provided dir
        csvlist = []
        colnames = ["name", "latitude", "longitude", "age"]
        for filename in cfilelist:
            print (filename)
            dataframe = pandas.read_csv(filename, header=None)
            csvlist.append(dataframe)
        mergedcsv = pandas.concat(csvlist, axis=0)
        print ("CSV Merge Completed!")
        print ("====================")
        print (mergedcsv)

        # add header to the final csv file
        mergedcsv.columns = colnames

        # output to the file defined
        mergedcsv.to_csv(outfile)
    else:
        print ("Merge is already done!")
        print ("Delete the following-"+outfile+" to re-merge available CSVs")

# def removeNaNValues(dataFrame):
#     """
#     Remove Null or NaN values from the CSV list
#
#     """
#     sanitizedDF = dataFrame.drop_duplicates()
#     sanitizedDF = dataFrame.dropna()
#     return sanitizedDF


def removeNaNValues(inFile = const.NaN_IN_FILE, outFile = const.NaN_OUT_FILE):
    """
    Remove Null or NaN values from the CSV list

    """
    data = pandas.read_csv(inFile)
    data.drop_duplicates()
    data.dropna().to_csv(outFile)


def countRowsInCsv(fileName):
    """ Determine the number of rows in the CSV"""
    inFile = fileName
    with open(inFile, "r") as f:
        reader = csv.reader(f, delimiter=",")
        data = list(reader)
        row_count = len(data)
        print row_count


def appendLocationToCsv(inFile = const.APND_IN_FILE, outFile = const.APND_OUT_FILE):
    """
    DO NOT USE THIS
    Utility to add an additional column 'location' to the csv data.
    This column is derived from the lt, ln coordinates using the reverse geocode feature of
    pygeocoder library. The column displays the city location closest to the given coordinates.
    It was noted in the initial trials that for certain coordinates provided None outputs and hence
    this function handles specific errors and deletes such rows from the csv list to ensure that
    only relevant data is used for searches.

    :param inFile:
    :param outFile:
    :return:
        uber file with additional location column derived from row lt, ln coordinates.
    """
    from requests.exceptions import SSLError
    with open(inFile, 'rb') as csvinput:
        with open(outFile, 'wb') as csvoutput:
            writer = csv.writer(csvoutput, lineterminator='\n')
            reader = csv.reader(csvinput)
            all = []
            row = next(reader)
            row.append('location')
            all.append(row)
            for row in reader:
                lt = float(row[3])
                ln = float(row[4])
                deleteRowFlag = False
                try:
                    loc = getUserLocation(lt, ln)
                except (SSLError, UnicodeEncodeError, ValueError) as e:
                    print '--------------'
                    print lt, ln
                    print ("Error {} ".format(e))
                    print '--------------'
                    deleteRowFlag = True
                    pass
                except EOFError:
                    print " [EOF]\nNo data available. Default assumed"
                    print 'EOF Error'
                    pass
                else:
                    if loc != None:
                        row.append(loc)
                    else:
                        deleteRowFlag = True
                if not deleteRowFlag:
                    all.append(row)

            writer.writerows(all)
            print 'Done. Check the file...'


if __name__ == '__main__':
    """
    Dev Tests
    """
    import sys

    #countRowsInCsv(os.path.join(os.getcwd(), 'resources/dmclean.csv'))
    #print (os.getcwdu())
    # print (os.path.join(os.getcwd(), 'resources/dmfinal.csv'))
    #
    # print os.path.dirname(os.path.join(os.getcwd(), 'resources/data'))
    #removeNaNValues()
    #countRowsInCsv()

    # lt1 = -15.19611
    # ln1 = 12.15222
    # lt2 = 21.1667
    # ln2 = 72.8333
    # from time import time
    # print '---------Sphere----------'
    # t0 = time()
    # determineDistance(lt1,ln1,lt2,ln2)
    # tt = time() - t0
    # print "[FP-LY-NY] Executed in {} seconds".format(round(tt, 3))
    # print '---------Haversine-----------'
    # t0 = time()
    # haversine(ln1,lt1,ln2,lt2)
    # tt = time() - t0
    # print "[FP-LY-NY] Executed in {} seconds".format(round(tt, 3))
    # print '----------geopy------------'
    # t0 = time()
    # getGeoDistance(lt1,ln1,lt2,ln2)
    # tt = time() - t0
    # print "[FP-LY-NY] Executed in {} seconds".format(round(tt, 3))
    # print '----------------------'

