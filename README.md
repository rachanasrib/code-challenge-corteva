# Code Challenge Template

Database - Postgresql

Server - Python Flask Server

Pre-requisites:

    1. Install Postgresql database
    2. Install all the python package dependencies using the below command in the root folder:
        pip install -r requirements.txt

Tables Schema:

    1. Weather Data Table
        CREATE TABLE IF NOT EXISTS public.weather_data
        (
            record_date date NOT NULL,
            max_temp integer,
            min_temp integer,
            precipitation integer,
            weather_station character(11) COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT weather_data_pkey PRIMARY KEY (record_date, weather_station)
        )
    
    
    2. Weather Data Logs Table        
        CREATE TABLE IF NOT EXISTS public.weather_logs
        (
            start_time timestamp without time zone NOT NULL,
            end_time timestamp without time zone NOT NULL,
            records integer NOT NULL,
            weather_station character(11) COLLATE pg_catalog."default" NOT NULL
        )
    
    3. Weather Stats Table
        CREATE TABLE IF NOT EXISTS public.weather_stats
        (
            weather_station character(11) COLLATE pg_catalog."default" NOT NULL,
            record_year smallint NOT NULL,
            avg_min_temp numeric,
            avg_max_temp numeric,
            avg_precipitation numeric,
            CONSTRAINT weather_stats_pkey PRIMARY KEY (record_year, weather_station)
        )

    4. Yield Data Table
        CREATE TABLE IF NOT EXISTS public.yield_data
        (
            record_year smallint NOT NULL,
            total_yield integer NOT NULL,
            CONSTRAINT yield_data_pkey PRIMARY KEY (record_year)
        )


Folder Structure:

    code-challenge-template
        i) src
            a) config.ini - Configuration file which holds postgresql database config
            b) main.py - Main python file that is responsible for the application flow
            c) database.py - Python file responsible for database operations
            d) data_fetch.py - Python file to featch data from input folders and preprocess the data
            e) server.py - Python file resposible to launch the Flask server and expose the required end points
        ii) wx_data - Input folder with weather station data files
        iii) yld_data - Input folder with yield data files
        iv) requirements.txt - File with python dependencies
        v) README.md
    

Application Flow:

    1. To install required dependencies please run the below command
        pip install - r requirements.txt
    2. To launch the application run the below command in the src folder
        python main.py
    3. The application can be launched in two modes i.e., only Flask server mode where we can make the GET requests and the second mode in which the data is injected into database initially and then the flask server is launched.
    4. In order to run the flask server alone enter 1, inorder to inject and run the flask server enter 2.
    5. Before running the flask server or performing data injection we establish databse connection.
    6. The database configurations are present inside config.ini file. Please configure the vavlues according to the local setup.
    7. In case of data injection we are following the below steps:
        a. Drop the existing tables in the database
        b. Create required tables
        c. Read data from input folder which is present in the same directory as that of src folder. The input folders should be named as wx_data and yld_data respectively.
        d. Once all the data is read, we inject these data into the database.
    8. Once we are done with the data injection step we start the flask server (In case we choose 1 in the initial launch option it runs the flask server directly without the aobve data injection steps).
    9. The flask server will be launched in host = 0.0.0.0 (127.0.0.1) and port = 8081. Incase these values should be changed then please refer to the app.run statement in server.py.
    (Don't use the default URL output shown in the terminal, use the api calls mentioned below)
    10. The flask server exposes three API endpoints
        a. /api/weather
            - Query Params (Optional for filter purpose):
                i) station_id :- weather station id
                ii) date :- Record date
                iii) limit :- Number of records to be returned for pagenation purpose
                iv) offset :- Position/index to start records from for pagenation purpose. We return limit number of records from the given offset.
            - In case both station_id and date are provided in the query params then there is no use of limit and offset as only one record will be returned.
            - Example API Requests with different filter options:
                i) http://localhost:8081/api/weather
                ii) http://localhost:8081/api/weather?offset=1&limit=1000&date=19900101
                iii) http://localhost:8081/api/weather?offset=1&limit=1000&station_id=USC00110072
                iv) http://localhost:8081/api/weather?date=19900101&station_id=USC00110072
                v) http://localhost:8081/api/weather?station_id=USC00110187
        b. /api/yield
            - Query Params (Optional for filter purpose):
                i) year :- Record year
                ii) limit :- Number of records to be returned for pagenation purpose
                iii) offset :- Position/index to start records from for pagenation purpose. We return limit number of records from the given offset.
            - In case year is provided in the query params then there is no use of limit and offset as only one record will be returned.
            - Example API Requests with different filter options:
                i) http://localhost:8081/api/yield
                ii) http://localhost:8081/api/yield?offset=1&limit=15
                iii) http://localhost:8081/api/yield?offset=1&limit=10&year=2000
                iv) http://localhost:8081/api/yield?year=1995
        c. /api/weather/stats
            - Query Params (Optional for filter purpose):
                i) station_id :- weather station id
                ii) year :- Record year
                iii) limit :- Number of records to be returned for pagenation purpose
                iv) offset :- Position/index to start records from for pagenation purpose. We return limit number of records from the given offset.
            - In case both station_id and year are provided in the query params then there is no use of limit and offset as only one record will be returned.
            - Example API Requests with different filter options:
                i) http://localhost:8081/api/weather/stats
                ii) http://localhost:8081/api/weather/stats?offset=1&limit=1000&date=2012
                iii) http://localhost:8081/api/weather/stats?offset=1000&limit=500&station_id=USC00110072
                iv) http://localhost:8081/api/weather/stats?date=1991&station_id=USC00110072
                v) http://localhost:8081/api/weather/stats?station_id=USC00110187
