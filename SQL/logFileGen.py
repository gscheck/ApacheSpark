from distutils.sysconfig import get_python_lib
from random import seed
from random import randint
import mysql.connector

#-----------------------------------------------------------------
#	Title: Log File Generator
#	Author: Garth Scheck/Vida Movahedi
#	Date: 4/23/2020
#-----------------------------------------------------------------


# connect to mySQL database
cnx = mysql.connector.connect(user='spark', password='spark',
                              host='127.0.0.1',
                              database='fraud_det')

# create cursor for queries
cursor = cnx.cursor()

# write header to log file
f = open('/mnt/d/temp/datalog.csv', 'a+')
f.write("vendor, city, state, distance, transCat, transType, amount\n")
f.close()

# set number of loop iterations
ITERATIONS = 10000000

# start loop
for x in range(ITERATIONS):
    for y in range(ITERATIONS):
        try:
            # get count of lcoations
            query = ("SELECT count(location_id) FROM location_t")
            cursor.execute(query)
            location = cursor.fetchone()

            # get random location IDfrom distutils.sysconfig import get_python_lib
from random import seed
from random import randint
import mysql.connector

#-----------------------------------------------------------------
#	Title: Log File Generator
#	Author: Garth Scheck/Vida Movahedi
#	Date: 4/23/2020
#-----------------------------------------------------------------


# connect to mySQL database
cnx = mysql.connector.connect(user='spark', password='spark',
                              host='127.0.0.1',
                              database='fraud_det')

# create cursor for queries
cursor = cnx.cursor()

# write header to log file
f = open('/mnt/d/temp/datalog.csv', 'a+')
f.write("vendor,city,state,distance,transCat,transType,amount\n")
f.close()

# set number of loop iterations
ITERATIONS = 10000000

# start loop
for x in range(ITERATIONS):
    for y in range(ITERATIONS):
        try:
            # get count of lcoations
            query = ("SELECT count(location_id) FROM location_t")
            cursor.execute(query)
            location = cursor.fetchone()

            # get random location ID
            locID = randint(1,location[0])

            # get location info
            query = ("SELECT location, city, state, distance FROM location_t WHERE location_id = %s")
            cursor.execute(query, (locID,))
            location = cursor.fetchone()

            # get count of categories
            query = ("SELECT count(cat_id) FROM transaction_cat_t")
            cursor.execute(query)
            cat = cursor.fetchone()

            # get random catetory ID
            catID = randint(1,cat[0])

            query = ("SELECT type FROM transaction_cat_t WHERE cat_id = %s")
            cursor.execute(query, (catID,))
            cat = cursor.fetchone()
        
            # get min and max amount info
            query = ("SELECT max(type_id) FROM transaction_type_t WHERE cat_id = %s")
            cursor.execute(query, (catID,))
            ttype = cursor.fetchone()
            maxVal = ttype[0]
        
		    # get random transaction type
            ttypeID = randint(1, maxVal)

		    # query data for log file
            query = ("SELECT trans_type FROM transaction_type_t WHERE type_id = %s AND cat_id = %s")
            cursor.execute(query, (ttypeID, catID))
            ttype = cursor.fetchone()

            query = "SELECT amount_max FROM transaction_type_t WHERE type_id = %s AND cat_id = %s"
            cursor.execute(query, (ttypeID, catID))
            maxAmnt = cursor.fetchone()

            query = "SELECT amount_min FROM transaction_type_t WHERE type_id = %s AND cat_id = %s"
            cursor.execute(query, (ttypeID, catID))
            minAmnt = cursor.fetchone()

		    # write results to log file
            if not(ttype is None) and not(maxAmnt is None) and not(minAmnt is None):
                query = "SELECT ROUND(RAND() * (%s-%s) + %s) FROM dual"
                cursor.execute(query,(maxAmnt[0], minAmnt[0], minAmnt[0]))
                amount = cursor.fetchone()
 
                f = open('/mnt/d/temp/datalog.csv', 'a+')
                f.write(", ".join([str(tups) for tups in location]) + ", " + cat[0] + ", " + ttype[0] +  ", " + str(amount[0]) + "\n")
                f.close()

		
        except Exception as e:
            print(e)

# clean up: close cursor and db connection
cursor.close()

cnx.close()
            locID = randint(1,location[0])

            # get location info
            query = ("SELECT location, city, state, distance FROM location_t WHERE location_id = %s")
            cursor.execute(query, (locID,))
            location = cursor.fetchone()

            # get count of categories
            query = ("SELECT count(cat_id) FROM transaction_cat_t")
            cursor.execute(query)
            cat = cursor.fetchone()

            # get random catetory ID
            catID = randint(1,cat[0])

            query = ("SELECT type FROM transaction_cat_t WHERE cat_id = %s")
            cursor.execute(query, (catID,))
            cat = cursor.fetchone()
        
            # get min and max amount info
            query = ("SELECT max(type_id) FROM transaction_type_t WHERE cat_id = %s")
            cursor.execute(query, (catID,))
            ttype = cursor.fetchone()
            maxVal = ttype[0]
        
            # get random transaction type
            ttypeID = randint(1, maxVal)

            # query data for log file
            query = ("SELECT trans_type FROM transaction_type_t WHERE type_id = %s AND cat_id = %s")
            cursor.execute(query, (ttypeID, catID))
            ttype = cursor.fetchone()

            query = "SELECT amount_max FROM transaction_type_t WHERE type_id = %s AND cat_id = %s"
            cursor.execute(query, (ttypeID, catID))
            maxAmnt = cursor.fetchone()

            query = "SELECT amount_min FROM transaction_type_t WHERE type_id = %s AND cat_id = %s"
            cursor.execute(query, (ttypeID, catID))
            minAmnt = cursor.fetchone()

            # write results to log file
            if not(ttype is None) and not(maxAmnt is None) and not(minAmnt is None):
                query = "SELECT ROUND(RAND() * (%s-%s) + %s) FROM dual"
                cursor.execute(query,(maxAmnt[0], minAmnt[0], minAmnt[0]))
                amount = cursor.fetchone()
 
    	        f = open('/mnt/d/temp/datalog.csv', 'a+')
    	        f.write(", ".join([str(tups) for tups in location]) + ", " + cat[0] + ", " + ttype[0] +  ", " + str(amount) + "\n")
    	        f.close()

		
        except Exception as e:
            print(e)

# clean up: close cursor and db connection
cursor.close()

cnx.close()
