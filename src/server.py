from flask import Flask, request, jsonify
import database


# Method to define all endpoints and start server
def start_server(conn):
    app = Flask(__name__)
    db_conn = conn

    # Method dto perform @Get(/api/weather)
    @app.route('/api/weather', methods=['GET'])
    def get_weather_data():
        # Read respective filter options
        args = request.args
        id = args.get("station_id", "", type=str)
        date_val = args.get("date", "", type=str)
        page_id = args.get("offset", 1, type=int)
        page_size = args.get("limit", 1000, type=int)
        # Get data from database
        records = database.get_weather_data(db_conn, id, date_val, page_id, page_size)
        return jsonify(records)

    # Method dto perform @Get(/api/yield)
    @app.route('/api/yield', methods=['GET'])
    def get_yield_data():
        # Read respective filter options
        args = request.args
        year_val = args.get("year", 0, type=int)
        page_id = args.get("offset", 1, type=int)
        page_size = args.get("limit", 5, type=int)
        # Get data from database
        records = database.get_yield_data(db_conn, year_val, page_id, page_size)
        return jsonify(records)

    # Method dto perform @Get(/api/weather/stats)
    @app.route('/api/weather/stats', methods=['GET'])
    def get_weather_stats():
        # Read respective filter options
        args = request.args
        id = args.get("station_id", "", type=str)
        year_val = args.get("year", 0, type=int)
        page_id = args.get("offset", 1, type=int)
        page_size = args.get("limit", 500, type=int)
        # Get data from database
        records = database.get_weather_stats(db_conn, id, year_val, page_id, page_size)
        return jsonify(records)

    # Start server on 0.0.0.0 host and 8081 port
    app.run(host='0.0.0.0', port=8081)
