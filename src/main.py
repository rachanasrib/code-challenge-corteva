import database
import data_fetch
import server
import pandas as pd


if __name__ == "__main__":
    # Run mode input
    print("Enter 1 to run only flask server. Enter 2 to reset and inject database database and then launch flask server")
    server_flag = input()
    # Create initial postgresql database adapters
    database.create_adapters()
    # Get connection to postgresql database
    db_conn = database.create_connection()
    if server_flag == "2":
        # Drop the schemas if already present
        database.drop_tables(db_conn)
        # Create required schemas
        database.create_tables(db_conn)

        # Weather station and yield input data folders respective paths
        path_wx = "../wx_data/"
        path_yx = "../yld_data/"
        # Read weather station data from all input files
        weather_df, weather_logs_df = data_fetch.get_weather_station_data(path_wx)
        # Inject the weather data into database
        database.insert_values(db_conn, weather_df, 'weather_data')
        # Inject the weather stations logs into database
        database.insert_values(db_conn, weather_logs_df, 'weather_logs')
        # Read yield data from all input files
        yield_df = data_fetch.get_yield_data(path_yx)
        # Inject the yield data into database
        database.insert_values(db_conn, yield_df, 'yield_data')

        # Fetch weather stats data from database
        stats_records = database.fetch_stats(db_conn)
        weather_stats_df = pd.DataFrame(stats_records, columns=['weather_station', 'record_year', 'Avg_min_temp', 'Avg_max_temp', 'Avg_precipitation'])
        # Insert weather stats data into database
        database.insert_values(db_conn, weather_stats_df, 'weather_stats')

    if server_flag == "1":
        # Start Flask Server
        server.start_server(db_conn)

    # Close DB connection
    database.close_connection(db_conn)
