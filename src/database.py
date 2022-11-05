import pandas as pd
import psycopg2
import configparser
import psycopg2.extras as extras
import numpy
from psycopg2.extensions import register_adapter, AsIs


# Postgresql database Adapter method
def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


# Postgresql database Adapter method
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


# Postgresql database Adapter method
def nan_to_null(f, _NULL=psycopg2.extensions.AsIs('NULL'), _Float=psycopg2.extensions.Float):
    if f != f:
        return _NULL
    else:
        return _Float(f)


# Method to initialize Postgresql database Adapter
def create_adapters():
    register_adapter(numpy.float64, adapt_numpy_float64)
    register_adapter(numpy.int64, adapt_numpy_int64)
    register_adapter(float, nan_to_null)


# Method to read database config from config.ini and create a connection to the database
def create_connection():
    config = configparser.ConfigParser()
    config.read('config.ini')
    conn = psycopg2.connect(
        host=config['postgresqlDB']['host'],
        user=config['postgresqlDB']['user'],
        password=config['postgresqlDB']['pass'],
        database=config['postgresqlDB']['db'],
        port=config['postgresqlDB']['port']
    )
    print('Opened database successfully')
    return conn


# Method to close database connection
def close_connection(conn):
    conn.close()


# Method to drop all the existing tables
def drop_tables(conn):
    cursor = conn.cursor()

    # Drop weather data table
    cursor.execute('''DROP TABLE IF EXISTS weather_data;''')
    # Drop yield data table
    cursor.execute('''DROP TABLE IF EXISTS yield_data;''')
    # Drop weather data logs table
    cursor.execute('''DROP TABLE IF EXISTS weather_logs;''')
    # Drop weather stats data table
    cursor.execute('''DROP TABLE IF EXISTS weather_stats;''')

    conn.commit()
    cursor.close()


# Method to create all required tables
def create_tables(conn):
    cursor = conn.cursor()

    # Create weather data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather_data
    (
        record_date Date NOT NULL,
        max_temp integer,
        min_temp integer,
        precipitation integer,
        weather_station character(11) NOT NULL,
        CONSTRAINT weather_data_pkey PRIMARY KEY (record_date, weather_station)
    );''')

    # Create yield data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS yield_data
    (
        record_year smallint NOT NULL,
        total_yield integer NOT NULL,
        CONSTRAINT yield_data_pkey PRIMARY KEY (record_year)
    );''')

    # Create weather data logs table
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather_logs
    (
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        records integer NOT NULL,
        weather_station character(11) NOT NULL
    );''')

    # Create weather stats data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather_stats
        (
            weather_station character(11) NOT NULL,
            record_year smallint NOT NULL,
            Avg_min_temp numeric,
            Avg_max_temp numeric,
            Avg_precipitation numeric,
            CONSTRAINT weather_stats_pkey PRIMARY KEY (record_year, weather_station)
        );''')
    conn.commit()
    cursor.close()


# Method to insert given dataframe values into respective table
def insert_values(conn, df, table):
    # Create data row tuples from dataframe
    tuples = [tuple(x) for x in df.to_numpy()]

    # Get list of column names to be inserted from dataframe
    cols = ','.join(list(df.columns))
    # SQL query to execute
    # Prepare insert query with table and column names
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        # Insert all tuples
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        # Error Handler
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


# Method to fetch weather stats data from weather data table
def fetch_stats(conn):
    cursor = conn.cursor()

    # Query to fetch weather stats
    cursor.execute('''SELECT weather_station,
    extract(year from record_date) AS record_year,
    AVG (min_temp) as Avg_min_temp,
    AVG (max_temp) as Avg_max_temp,
    AVG (precipitation) as Avg_precipitation
    FROM weather_data GROUP BY
    weather_station, record_year order by weather_station, record_year;''')

    stats_records = cursor.fetchall()
    return stats_records


# Method to get weather data based on user filters
def get_weather_data(conn, id_val, date_val, page_id, page_size):
    cursor = conn.cursor()
    # Condition if no filters are provided
    if len(id_val) == 0 and len(date_val) == 0:
        cursor.execute('''SELECT * FROM weather_data ORDER BY weather_station, record_date LIMIT {limit} offset {offset};'''.format(limit=page_size, offset=((page_id - 1) * page_size)))
    # Condition if both date and weather station id are provided
    elif len(id_val) > 0 and len(date_val) > 0:
        cursor.execute("SELECT * FROM weather_data WHERE weather_station='" + id_val + "' AND record_date=TO_DATE('" + date_val + "', 'YYYYMMDD') ORDER BY weather_station, record_date;")
    # Condition when only weather station id is provided
    elif len(id_val) > 0:
        cursor.execute(("SELECT * FROM weather_data WHERE weather_station='" + id_val + "' ORDER BY weather_station, record_date LIMIT {limit} offset {offset};").format(limit=page_size, offset=((page_id - 1) * page_size)))
    # Condition when only date is provided
    else:
        cursor.execute(("SELECT * FROM weather_data WHERE record_date=TO_DATE('" + date_val + "', 'YYYYMMDD') ORDER BY weather_station, record_date LIMIT {limit} offset {offset};").format(limit=page_size, offset=((page_id - 1) * page_size)))
    weather_data_records = cursor.fetchall()
    df = pd.DataFrame(weather_data_records, columns=['record_date', 'max_temp', 'min_temp', 'precipitation', 'weather_station'])
    return df.to_json(orient='records')


# Method to get yield data based on user filters
def get_yield_data(conn, year_val, page_id, page_size):
    cursor = conn.cursor()
    # Condition if no filters are provided
    if year_val == 0:
        cursor.execute('''SELECT * FROM yield_data ORDER BY record_year LIMIT {limit} offset {offset};'''.format(limit=page_size, offset=((page_id - 1) * page_size)))
    # Condition if year is provided
    else:
        cursor.execute('SELECT * FROM yield_data WHERE record_year=' + str(year_val) + ';')
    yield_data_records = cursor.fetchall()
    df = pd.DataFrame(yield_data_records, columns=['record_year', 'total_yield'])
    return df.to_json(orient='records')


# Method to get weather stats data based on user filters
def get_weather_stats(conn, id_val, year_val, page_id, page_size):
    cursor = conn.cursor()
    # Condition if no filters are provided
    if len(id_val) == 0 and year_val == 0:
        cursor.execute('''SELECT * FROM weather_stats ORDER BY weather_station, record_year LIMIT {limit} offset {offset};'''.format(limit=page_size, offset=((page_id - 1) * page_size)))
    # Condition if both yeah and weather station id are provided
    elif len(id_val) > 0 and year_val > 0:
        cursor.execute("SELECT * FROM weather_stats WHERE weather_stats=" + id_val + "' AND record_year=" + str(year_val) + ' ORDER BY weather_station, record_year;')
    # Condition if only weather station id is provided
    elif len(id_val) > 0:
        cursor.execute(("SELECT * FROM weather_stats WHERE weather_station='" + id_val + "' ORDER BY weather_station, record_year LIMIT {limit} offset {offset};").format(limit=page_size, offset=((page_id - 1) * page_size)))
    # Condition if only year is provided
    else:
        cursor.execute(('SELECT * FROM weather_stats WHERE record_year=' + str(year_val) + ' ORDER BY weather_station, record_year LIMIT {limit} offset {offset};').format(limit=page_size, offset=((page_id - 1) * page_size)))
    weather_stats_records = cursor.fetchall()
    df = pd.DataFrame(weather_stats_records, columns=['weather_station', 'record_year', 'avg_min_temp', 'avg_max_temp', 'avg_precipitation'])
    return df.to_json(orient='records')
