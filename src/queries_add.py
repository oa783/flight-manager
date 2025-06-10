import sqlite3
from typing import Optional
import pandas as pd
from database import get_connection
from models import (
    ValidationError, DatabaseError,
    validate_date, validate_datetime, validate_airport_code, validate_flight_status, validate_rank, validate_positive_number,
    validate_flight_number, validate_licence_number
)
from utils import format_summary_table


def add_airport(code: str, name: str, city: str, country: str,
                utc_offset: float, tz_name: str) -> None:
    """
    Add a new airport record to the database after validating inputs
    """

    try:
        code = validate_airport_code(code)  # Ensure airport code is valid
        
        with get_connection() as conn:
            cur = conn.cursor()
            # Insert new airport details
            cur.execute("""
                INSERT INTO Airport (airport_code, name, city, country, utc_offset, tz_name) VALUES (?, ?, ?, ?, ?, ?)""", (code, name, city, country, utc_offset, tz_name))
            conn.commit()
            print(f"Airport  {code} - {name}  added successfully")
            
    except ValidationError as e: # Input validation error
        print(str(e))
    except sqlite3.IntegrityError: # Duplicate airport entry
        print(f"Airport {code} already exists")
    except DatabaseError as e: # Other database error
        print(str(e))


def add_route(origin: str, dest: str, distance_km: float, flight_duration_mins: int) -> None:
    """
    Add a new flight route between two airports with distance and duration
    """

    try:
        # Validate inputs first
        origin = validate_airport_code(origin)
        dest = validate_airport_code(dest)
        distance_km = validate_positive_number(distance_km, "Distance")
        flight_duration_mins = int(validate_positive_number(flight_duration_mins, "Flight duration"))
        
        if origin == dest:
            raise ValidationError("Origin and destination must be different")
        
        with get_connection() as conn:
            cur = conn.cursor()
            

            # Verify that the airports exist
            for code in (origin, dest):
                if not cur.execute("SELECT 1 FROM Airport WHERE airport_code=?", (code,)).fetchone():
                    raise ValidationError(f"Airport {code} not found")
            
            # Now insert the new route
            cur.execute("""
                INSERT INTO Route (origin_code, dest_code, distance_km, flight_duration_mins) VALUES (?, ?, ?, ?)""", (origin, dest, distance_km, flight_duration_mins))
            conn.commit()
            print(f"Route {origin} -> {dest} added successfully")
            

    except ValidationError as e:
        print(str(e))
    except sqlite3.IntegrityError:
        print(f"Route {origin} -> {dest} already exists")
    except DatabaseError as e:
        print(str(e))


def add_pilot(licence: str, first_name: str, last_name: str,
              rank: str, hire_date: str) -> None:
    """
    Add a new pilot to the database after validating rank, hire date, and licence number
    """

    try:
        # Validate fields
        rank = validate_rank(rank)
        hire_date = validate_date(hire_date)
        licence = validate_licence_number(licence)
        
        with get_connection() as conn:
            cur = conn.cursor()
            # Insert new pilot details
            cur.execute("""INSERT INTO Pilot (licence_no, first_name, last_name, rank, hire_date)
                VALUES (?, ?, ?, ?, ?)
            """, (licence, first_name, last_name, rank, hire_date))
            conn.commit()
            print(f"{rank} {first_name} {last_name} added successfully. Happy flying")
            
    except ValidationError as e:
        print(str(e))
    except sqlite3.IntegrityError:
        print(f"Licence {licence} already exists")
    except DatabaseError as e:
        print(str(e))


def add_flight(flight_number: str, flight_date: str, route_id: int,
               sched_dep_utc: str, sched_arr_utc: str,
               status: str = "Scheduled") -> None:
    """
    Add a new flight to the database after validating all flight details
    """

    try:
        flight_date = validate_date(flight_date)
        sched_dep_utc = validate_datetime(sched_dep_utc)
        sched_arr_utc = validate_datetime(sched_arr_utc)
        status = validate_flight_status(status)
        flight_number = validate_flight_number(flight_number)
        
        if sched_dep_utc >= sched_arr_utc:
            raise ValidationError("Departure time must be before arrival time!")
        
        with get_connection() as conn:
            cur = conn.cursor()
            
            # Verify route exists
            route = cur.execute(
                "SELECT origin_code, dest_code FROM Route WHERE route_id=?",
                (route_id,)
            ).fetchone()
            
            if not route:
                raise ValidationError(f"Route ID {route_id} not found")
            
            # Get status ID
            sid = cur.execute(
                "SELECT status_id FROM FlightStatus WHERE status_name=?",
                (status,)
            ).fetchone()[0]
            
            cur.execute("""
                INSERT INTO Flight (flight_number, flight_date, route_id,
                                  sched_dep_utc, sched_arr_utc, status_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (flight_number, flight_date, route_id, sched_dep_utc, sched_arr_utc, sid))
            
            conn.commit()
            print(f"Flight {flight_number} on {flight_date} ({route[0]} -> {route[1]}) added successfully")
            
    except ValidationError as e:
        print(str(e))
    except sqlite3.IntegrityError:
        print(f"Flight {flight_number} on {flight_date} already exists")
    except DatabaseError as e:
        print(str(e))


def assign_captain(flight_id: int, pilot_id: int) -> None:
    """
    Assign a pilot as the captain of a given flight
    If a captain is already assigned, replace them
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            
            # Verify flight exists
            flight = cur.execute(
                "SELECT flight_number, flight_date FROM Flight WHERE flight_id=?",
                (flight_id,)
            ).fetchone()
            
            if not flight:
                raise ValidationError(f"Flight ID {flight_id} not found")
            
            # Verify pilot exists and is a captain
            pilot = cur.execute(
                "SELECT first_name, last_name, rank FROM Pilot WHERE pilot_id=?",
                (pilot_id,)
            ).fetchone()
            
            if not pilot:
                raise ValidationError(f"Pilot ID {pilot_id} not found")
            
            if pilot[2] != 'Captain':
                raise ValidationError(f"{pilot[0]} {pilot[1]} is not a Captain")
            
            # Remove existing captain if any
            cur.execute(
                "DELETE FROM CrewAssignment WHERE flight_id=? AND role='Captain'",
                (flight_id,)
            )
            
            # Assign new captain
            cur.execute("""
                INSERT INTO CrewAssignment (flight_id, pilot_id, role)
                VALUES (?, ?, 'Captain')
            """, (flight_id, pilot_id))
            
            conn.commit()
            print(f"Captain {pilot[0]} {pilot[1]} assigned to flight {flight[0]} on {flight[1]}")
            
    except ValidationError as e:
        print(str(e))
    except sqlite3.IntegrityError:
        print("Assignment failed - pilot may already be assigned to this flight")
    except DatabaseError as e:
        print(str(e))


# Summary queries
def run_summary_query(query: str, params: Optional[dict] = None, title: str = "Results") -> None:
    """
    Run a SQL query and format the results as a table

    Args:
        query (str): SQL query 
        params (Optional[dict]): Parameters to substitute into the query
        title (str): Title to display above the table
    """

    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params or {})
            format_summary_table(df, title)
    except Exception as e:
        print(f"Query failed unexpectdly: {e}")


def flights_per_destination():
    """Show the number of the flights to each destination"""
    query = """
    SELECT 
        dest_code AS "Destination", 
        COUNT(*) AS "Total Flights"
    FROM v_flight_details
    GROUP BY dest_code
    ORDER BY COUNT(*) DESC;
    """
    run_summary_query(query, title="Flights per Destination")


def flights_per_destination_date_range(date_from: str, date_to: str):
    """ Show the number of flights to each destination within a certain date range"""
    try:
        date_from = validate_date(date_from)
        date_to = validate_date(date_to)
        
        query = """
        SELECT 
            dest_code AS "Destination", 
            COUNT(*) AS "Flights"
        FROM v_flight_details
        WHERE flight_date BETWEEN :from AND :to
        GROUP BY dest_code
        ORDER BY COUNT(*) DESC;
        """
        run_summary_query(
            query, 
            {"from": date_from, "to": date_to}, 
            f"Flights per Destination ({date_from} to {date_to})"
        )
    except ValidationError as e:
        print(str(e))


def flights_per_pilot():
    """Show flights assigned to each pilot"""
    query = """
    SELECT 
        p.pilot_id AS "ID",
        p.first_name || ' ' || p.last_name AS "Pilot Name",
        p.rank AS "Rank",
        COUNT(*) AS "Flights Assigned"
    FROM CrewAssignment ca
    JOIN Pilot p ON ca.pilot_id = p.pilot_id
    GROUP BY p.pilot_id, p.first_name, p.last_name, p.rank
    ORDER BY COUNT(*) DESC;
    """
    run_summary_query(query, title="Flights per Pilot")


def flights_by_status():
    """Show current flight statuses"""
    query = """
    SELECT 
        status_name AS "Status", 
        COUNT(*) AS "Number of Flights"
    FROM v_flight_details
    GROUP BY status_name
    ORDER BY COUNT(*) DESC;
    """
    run_summary_query(query, title="Flights by Status")


def top_busiest_routes(limit: int = 10):
    """Show the top X busiest routes"""
    query = """
    SELECT 
        origin_code || ' -> ' || dest_code AS "Route",
        COUNT(*) AS "Number of Flights"
    FROM v_flight_details
    GROUP BY origin_code, dest_code
    ORDER BY COUNT(*) DESC
    LIMIT :limit;
    """
    run_summary_query(query, {"limit": limit}, f"Top {limit} Busiest Routes")


def show_all_pilots():
    """Display all pilots """
    query = """
    SELECT 
        pilot_id AS "ID",
        licence_no AS "Licence",
        first_name || ' ' || p.last_name AS "Name",
        rank AS "Rank",
        hire_date AS "Hire Date"
    FROM Pilot p
    ORDER BY rank, last_name, first_name;
    """
    run_summary_query(query, title="All Pilots")


def show_all_airports():
    """Display all airports """
    query = """
    SELECT 
        airport_code AS "Code",
        name AS "Airport Name",
        city AS "City",
        country AS "Country",
        utc_offset AS "UTC Offset"
    FROM Airport
    ORDER BY country, city;
    """
    run_summary_query(query, title="All Airports")


def show_all_routes():
    """Display all routes in the system"""
    query = """
    SELECT 
        route_id AS "ID",
        origin_code || ' -> ' || dest_code AS "Route",
        distance_km AS "Distance (km)",
        flight_duration_mins AS "Duration (min)"
    FROM Route
    ORDER BY origin_code, dest_code;
    """
    run_summary_query(query, title="All Routes")


def show_pilots_by_rank(rank: str):
    """
    Display a list of pilots filtered by rank
    """

    query = """
    SELECT pilot_id, first_name || ' ' || last_name AS name
    FROM Pilot
    WHERE rank = ?
    ORDER BY last_name, first_name;
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, (rank,))
            pilots = cur.fetchall()
            for pilot in pilots:
                print(f"  ID: {pilot[0]:3d} - {pilot[1]}")
    except Exception as e:
        print(f"Failed to load pilots unexpectedly: {e}")