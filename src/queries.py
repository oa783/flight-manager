"""
Database queries and operations for operations and modifications
"""

import sqlite3
from typing import Optional, List
import pandas as pd
from database import get_connection
from models import (
    ValidationError, DatabaseError,
    validate_date, validate_datetime, validate_airport_code,
    validate_flight_status,
    validate_flight_number, 
)
from utils import (
    format_preview, confirm_action
)


def get_flights(origin_code: Optional[str] = None,  dest_code: Optional[str] = None,
                status_name: Optional[str] = None,
                date_from: Optional[str] = None,
                date_to: Optional[str] = None,
                captain_id: Optional[int] = None):
    
    """
    Retrieve flights based on optional filter criteria like origin, destination, date range, status, and captain ID
    Returns a list of matching flight records
    """

    try:
        # Validate filter inputs if provided by the user
        if origin_code:
            origin_code = validate_airport_code(origin_code)
        if dest_code:
            dest_code = validate_airport_code(dest_code)
        if status_name:
            status_name = validate_flight_status(status_name)
        if date_from:
            date_from = validate_date(date_from)
        if date_to:
            date_to = validate_date(date_to)


        # SQL query with optional filters (utilising NULL-safe filtering)
        query = """

        SELECT flight_id, flight_number, flight_date,
               origin_code, dest_code,
               status_name,
               sched_dep_utc, sched_arr_utc,
               captain_name, captain_id,
               fo_name, fo_id
        FROM   v_flight_details
        WHERE  (:origin IS NULL OR origin_code = :origin)
           AND (:dest   IS NULL OR dest_code   = :dest)
           AND (:stat   IS NULL OR status_name = :stat)
           AND (:dfrom  IS NULL OR flight_date >= :dfrom)
           AND (:dto    IS NULL OR flight_date <= :dto)
           AND (:cap    IS NULL OR captain_id  = :cap)
        ORDER  BY flight_date, sched_dep_utc;
        """
        
        params = {
            "origin": origin_code,
            "dest": dest_code,
            "stat": status_name,
            "dfrom": date_from,
            "dto": date_to,
            "cap": captain_id,
        }
        
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            return cur.fetchall()
            
    except ValidationError as e:
        print(str(e))
        return []
    except DatabaseError as e:
        print(str(e))
        return []


def _load_flight(flight_number: str, flight_date: str, conn: sqlite3.Connection) -> dict:
    """
    Load a specific flight from the database given its flight number and date
    """

    flight_number = validate_flight_number(flight_number)
    cur = conn.cursor()
    cur.execute("""
        SELECT flight_id, flight_number, flight_date,
               sched_dep_utc, sched_arr_utc,
               status_name, origin_code, dest_code,
               captain_name, captain_id, fo_name, fo_id
        FROM v_flight_details
        WHERE flight_number = ? AND flight_date = ?
    """, (flight_number, flight_date))
    row = cur.fetchone()
    if not row:
        raise ValidationError(f"No flight found for {flight_number} on {flight_date}")
    return dict(row)


def change_route(flight_number: str, flight_date: str, new_origin: str, new_dest: str) -> None:
    """
    Change the route for a flight
    creates a new route if it doesn't already exist
    """

    try:
        # Validate new origin and destination codes
        new_origin = validate_airport_code(new_origin)
        new_dest = validate_airport_code(new_dest)
        flight_date = validate_date(flight_date)
        
        with get_connection() as conn:
            conn.execute("BEGIN")  # Start transaction
            flight = _load_flight(flight_number, flight_date, conn) # Load current flight details
            
             # Prepare proposed changes for user preview
            proposed = flight.copy()
            proposed["origin_code"] = new_origin
            proposed["dest_code"] = new_dest
            
            format_preview(flight, proposed)  # Show old vs new details

            if not confirm_action():
                conn.rollback()
                print("Route change cancelled ")
                return
            cur = conn.cursor()
            # Check if the new route exists
            route_id = cur.execute(
                "SELECT route_id FROM Route WHERE origin_code=? AND dest_code=?",
                (new_origin, new_dest)
            ).fetchone()
            
            if route_id:
                route_id = route_id[0] # the existing route
            else:
                # Create new route with placeholders for distance/duration
                cur.execute("""
                    INSERT INTO Route (origin_code, dest_code, distance_km, flight_duration_mins)
                    VALUES (?, ?, 0, 0)
                """, (new_origin, new_dest))
                route_id = cur.lastrowid
                print(f"Created new route {new_origin} -> {new_dest}")
            
            # Update the flight with the new route_id
            cur.execute("UPDATE Flight SET route_id=? WHERE flight_id=?",
                       (route_id, flight["flight_id"]))
            conn.commit()
            print("Route updated successfully")
            
    except (ValidationError, DatabaseError) as e:
        print(str(e))


def change_times(flight_number: str, flight_date: str, new_dep: str, new_arr: str) -> None:
    """
    Change the scheduled departure and arrival times for a flight
    """

    try:
        # Validate the new times
        new_dep = validate_datetime(new_dep)
        new_arr = validate_datetime(new_arr)
        flight_date = validate_date(flight_date)
        
        if new_dep >= new_arr:
            raise ValidationError("Departure time must be before arrival time")
        
        with get_connection() as conn:
            conn.execute("BEGIN")
            flight = _load_flight(flight_number, flight_date, conn)
            
            # Copy existing schedule and update df, without impacting Database
            proposed = flight.copy()
            proposed["sched_dep_utc"] = new_dep
            proposed["sched_arr_utc"] = new_arr
            
            format_preview(flight, proposed)
            if not confirm_action():
                conn.rollback()
                print("Time change cancelled.")
                return
            
            cur = conn.cursor()
            # Update the flight times in database
            cur.execute("""
                UPDATE Flight
                SET sched_dep_utc=?, sched_arr_utc=?
                WHERE flight_id=?
            """, (new_dep, new_arr, flight["flight_id"]))
            conn.commit()
            print("Times updated successfully")
            
    except (ValidationError, DatabaseError) as e:
        print(str(e))


def change_status(flight_number: str, flight_date: str, new_status: str) -> None:
    """
    Change the flight status for a flight
    """

    try:
        new_status = validate_flight_status(new_status)
        flight_date = validate_date(flight_date)
        
        with get_connection() as conn:
            cur = conn.cursor()
            # Get the status_id for the new status
            sid = cur.execute(
                "SELECT status_id FROM FlightStatus WHERE status_name=?",
                (new_status,)
            ).fetchone()
            
            if not sid:
                raise ValidationError(f"Invalid status: {new_status}")
            sid = sid[0]
            
            conn.execute("BEGIN")  # Start transaction
            flight = _load_flight(flight_number, flight_date, conn)
            
            proposed = flight.copy()
            proposed["status_name"] = new_status
            
            format_preview(flight, proposed) # Show status change
            if not confirm_action():
                conn.rollback()
                print("Status change cancelled.")
                return
            # Update the flight's status_id 
            cur.execute("UPDATE Flight SET status_id=? WHERE flight_id=?",
                       (sid, flight["flight_id"]))
            conn.commit()
            print("Status updated successfully")
            
    except (ValidationError, DatabaseError) as e:
        print(str(e))


def change_captain(flight_number: str, flight_date: str, new_pilot_id: int) -> None:
    """
    Reassign a captain to a flight
    """

    try:
        flight_date = validate_date(flight_date)
        
        with get_connection() as conn:
            cur = conn.cursor()
            # Verify the pilot exists and is a captain
            pilot = cur.execute(
                "SELECT first_name, last_name, rank FROM Pilot WHERE pilot_id=?",
                (new_pilot_id,)
            ).fetchone()
            
            if not pilot:
                raise ValidationError(f"Pilot with ID {new_pilot_id} not found")
            
            if pilot[2] != 'Captain':
                raise ValidationError(f"{pilot[0]} {pilot[1]} is not a Captain")
            
            conn.execute("BEGIN") # Start transaction
            flight = _load_flight(flight_number, flight_date, conn)
            
            proposed = flight.copy()
            proposed["captain_name"] = f"{pilot[0]} {pilot[1]}"
            proposed["captain_id"] = new_pilot_id
            
            format_preview(flight, proposed) # Preview captain change
            if not confirm_action():
                conn.rollback()
                print("Captain change cancelled.")
                return
            
             # Remove existing captain
            cur.execute("DELETE FROM CrewAssignment WHERE flight_id=? AND role='Captain'",
                       (flight["flight_id"],))
            
            # And assign new captain
            cur.execute("""
                INSERT INTO CrewAssignment (flight_id, pilot_id, role)
                VALUES (?, ?, 'Captain')
            """, (flight["flight_id"], new_pilot_id))
            
            conn.commit()
            print("Captain reassigned successfully")
            
    except (ValidationError, DatabaseError) as e:
        print(str(e))


def change_first_officer(flight_number: str, flight_date: str, new_pilot_id: int) -> None:
    """
    Reassign a new first officer to a flight
    """

    try:
        flight_date = validate_date(flight_date)
        
        with get_connection() as conn:
            cur = conn.cursor()

            # Verify the pilot exists and is a first officer
            pilot = cur.execute(
                "SELECT first_name, last_name, rank FROM Pilot WHERE pilot_id=?",
                (new_pilot_id,)).fetchone()
            
            if not pilot:
                raise ValidationError(f"Pilot with ID {new_pilot_id} not found")
            
            if pilot[2] != 'First Officer':
                raise ValidationError(f"{pilot[0]} {pilot[1]} is not a First Officer")
            
            conn.execute("BEGIN")
            flight = _load_flight(flight_number, flight_date, conn)
            
            proposed = flight.copy()
            proposed["fo_name"] = f"{pilot[0]} {pilot[1]}"
            proposed["fo_id"] = new_pilot_id
            
            format_preview(flight, proposed)
            if not confirm_action():
                conn.rollback()
                print("First Officer change cancelled")
                return
            
            # Remove existing first officer
            cur.execute("DELETE FROM CrewAssignment WHERE flight_id=? AND role='First Officer'",
                       (flight["flight_id"],))
            
            # Assign new first officer
            cur.execute("""
                INSERT INTO CrewAssignment (flight_id, pilot_id, role)
                VALUES (?, ?, 'First Officer')
            """, (flight["flight_id"], new_pilot_id))
            
            conn.commit()
            print("First Officer changed successfully")
            
    except (ValidationError, DatabaseError) as e:
        print(str(e))