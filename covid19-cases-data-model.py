'''
Created on 9 Feb 2023

@author: ariel
'''
import psycopg2
import pandas as pd
import mysql.connector
print('packages imported successfully!')

def create_database():
    #connect to Mysql DB
    try:
        conn = mysql.connector.connect(user='root', password='mn383301534',
                                  host='localhost',
                                  database='datamodel')
        print('connected successfully')
    except:
        print('Error: could not make connection to the Mysql database')
    
        
    #build the cursor to execute sql queries in the DB
    try:
        cur = conn.cursor()
        print('cursor built successfully!')
    except:
        print('Error: Could not get cursor to the Database!')
    
        
    #set automatic commit to be True so that each action is committed without having to call conn.commit() after each command
    conn.autocommit=True
    print('autocommit set successfully!')
    
    #create database named accounts
    cur.execute('''DROP DATABASE IF EXISTS covid''')
    print('There is no covid database anymore!')
    cur.execute('CREATE DATABASE covid')
    print('Database covid created successfully!')
    
    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('tables drop successfully!')
    
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('tables created successfully!')
    
if __name__=='__main__':
    
    #read all tables from csv format
    enigma_jhud = pd.read_csv("C:/Users/ariel/Desktop/AWS Project/enigma-jhud/Enigma-JHU.csv.gz")
    print('csv file 1 read successfully!')

    factCovid_1 = enigma_jhud[['fips', 'province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active']]
    print('factCovid_1:\n', factCovid_1.head())
    
    rearc_test_data_states_daily = pd.read_csv("C:/Users/ariel/Desktop/AWS Project/rearc-covid-19-testing-data/states_daily.csv")
    print('csv file 2 read successfully!')

    factCovid_2 = rearc_test_data_states_daily[['fips', 'date', 'positive', 'negative', 'hospitalized', 'hospitalizedDischarged']]
    print('factCovid_2:\n', factCovid_2.head())
    
    enigma_nytimes_us_country = pd.read_csv("C:/Users/ariel/Desktop/AWS Project/enigma-nytimes-data-in-usa/us_county.csv")
    print('csv file 3 read successfully!')
    
    dimRegion_1 = enigma_jhud[['fips', 'province_state', 'country_region', 'latitude', 'longitude']]
    print('dimRegion_1:\n', dimRegion_1.head())
    
    dimRegion_2 = enigma_nytimes_us_country[['fips', 'county', 'state']]
    print('dimRegion_2:\n', dimRegion_2.head())
    
    #connect to database and create a new one
    cur, conn = create_database()
    
    #create table structure in database
    factCovid_1_table_create = ('''CREATE TABLE IF NOT EXISTS covid.factCovid_1(fips varchar(10), province_state varchar(10), \
    country_region varchar(10), confirmed varchar(10), deaths varchar(10), recovered varchar(10), active varchar(10))''')
    
    cur.execute(factCovid_1_table_create)
    print('table 1 in SQL created successfully!')
    
    factCovid_2_table_create = ('''CREATE TABLE IF NOT EXISTS covid.factCovid_2(fips varchar(10), date varchar(10), \
    positive varchar(10), negative varchar(10), hospitalized varchar(10), hospitalizedDischarged varchar(10))''')
    
    cur.execute(factCovid_2_table_create)
    print('table 2 in SQL created successfully!')
    
    dimRegion_1_table_create = ('''CREATE TABLE IF NOT EXISTS covid.dimRegion_1(fips varchar(10), province_state varchar(10), \
    country_region varchar(10), latitude varchar(10), longitude varchar(10))''')
    
    cur.execute(dimRegion_1_table_create)
    print('table 3 in SQL created successfully!')
    
    dimRegion_2_table_create = ('''CREATE TABLE IF NOT EXISTS covid.dimRegion_2(fips varchar(10), county varchar(10), \
    state varchar(10))''')
    
    cur.execute(dimRegion_2_table_create)
    print('table 4 in SQL created successfully!')
    
    
    #insert data into the Database
    print('*******************')
    factCovid_1_table_insert = ('''INSERT INTO covid.factCovid_1(fips, province_state, country_region, \
    confirmed, deaths, recovered, active) VALUES(%s, %s, %s, %s, %s, %s, %s)''')
    for i, row in factCovid_1.iterrows():
        cur.execute(factCovid_1_table_insert, list(row))
    print('data1 inserted successfully!')


    print('*******************')
    factCovid_2_table_insert = ('''INSERT INTO covid.factCovid_2(fips, date, positive, negative, 
    hospitalized, hospitalizedDischarged) VALUES(%s, %s, %s, %s, %s, %s)''')
    for i, row in factCovid_2.iterrows():
        cur.execute(factCovid_2_table_insert, list(row))
    print('data2 inserted successfully!')

    print('*******************')
    dimRegion_1_table_insert = ('''INSERT INTO covid.dimRegion_1(fips, province_state, country_region, \
    latitude, longitude) VALUES(%s, %s, %s, %s, %s)''')
    for i, row in dimRegion_1.iterrows():
        cur.execute(dimRegion_1_table_insert, list(row))
    print('data3 inserted successfully!')
    
    print('*******************')
    dimRegion_2_table_insert = ('''INSERT INTO covid.dimRegion_2(fips, county, state) VALUES(%s, %s, %s)''')
    for i, row in dimRegion_2.iterrows():
        cur.execute(dimRegion_2_table_insert, list(row))
    print('data4 inserted successfully!')