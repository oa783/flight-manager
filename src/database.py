"""
Initialise database and handle connection management for the Flight Management System
"""

import sqlite3
import os
from contextlib import contextmanager
from config import DB_PATH
from models import DatabaseError


@contextmanager
def get_connection():
    """Context manager for database connection
    """

    conn = sqlite3.connect(DB_PATH) # Open connection to the database
    conn.row_factory = sqlite3.Row # Return rows as dictionaries
    conn.execute("PRAGMA foreign_keys = ON;") # Enable foreign key constraints
    try: 
        yield conn # Provide the connection 
    except sqlite3.Error as e:
        conn.rollback() # Roll back transaction on error
        raise DatabaseError(f"Database Error: {e}")
    finally:
        conn.close() # close the connection


def initialise_tables() -> None:
    """Initialise the database by creating tables and seeding initial data."""

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH) # Remove existing database file to start fresh
    try:
        with get_connection() as conn:
            _create_tables(conn) # Create  tables
            _generate_data(conn) # Insert initial seed data
            conn.commit() # Commit all changes
    except Exception as e: 
        print(f"Database Error: {e}") # If error raise...
        raise


def _create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor() # Create a cursor object to execute SQL statements
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS Airport(
            airport_code TEXT PRIMARY KEY CHECK(length(airport_code) = 3),
            name TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            utc_offset REAL NOT NULL,
            tz_name TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Route(
            route_id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_code TEXT NOT NULL,
            dest_code TEXT NOT NULL,
            distance_km REAL NOT NULL,
            flight_duration_mins INTEGER NOT NULL,
            FOREIGN KEY (origin_code) REFERENCES Airport(airport_code) ON DELETE RESTRICT,
            FOREIGN KEY (dest_code) REFERENCES Airport(airport_code) ON DELETE RESTRICT,
            UNIQUE (origin_code, dest_code)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Pilot(
            pilot_id INTEGER PRIMARY KEY autoincrement,
            licence_no TEXT UNIQUE NOT NULL ,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            rank TEXT NOT NULL CHECK (rank IN ('Captain', 'First Officer')),
            hire_date DATE NOT NULL
        );
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pilot_licence_clean
        ON Pilot (UPPER(TRIM(licence_no)));
        """,
        """
        CREATE TABLE IF NOT EXISTS FlightStatus (
            status_id INTEGER PRIMARY KEY,
            status_name TEXT UNIQUE NOT NULL
            CHECK (status_name IN ('Scheduled','Boarding','Departed','Cancelled','Delayed'))
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Flight(
            flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_number TEXT NOT NULL,
            flight_date DATE NOT NULL,
            route_id INTEGER NOT NULL,
            sched_dep_utc DATETIME NOT NULL ,
            sched_arr_utc DATETIME NOT NULL CHECK (datetime(sched_arr_utc) > datetime(sched_dep_utc)),
            status_id INTEGER NOT NULL,
            FOREIGN KEY (route_id) REFERENCES Route(route_id) ON DELETE RESTRICT,
            FOREIGN KEY (status_id) REFERENCES FlightStatus(status_id) ON DELETE RESTRICT,
            UNIQUE (flight_number, flight_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS CrewAssignment (
            flight_id INTEGER NOT NULL ,
            pilot_id INTEGER NOT NULL,
            role TEXT CHECK(role IN ('Captain','First Officer')),
            PRIMARY KEY (flight_id, pilot_id),
            FOREIGN KEY (flight_id) REFERENCES Flight(flight_id) ON DELETE CASCADE,
            FOREIGN KEY (pilot_id) REFERENCES Pilot(pilot_id) ON DELETE CASCADE
        );
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_flight_role_unique
        ON CrewAssignment (flight_id, role);
        """,
        """
        CREATE VIEW IF NOT EXISTS v_flight_details AS
        SELECT
            f.flight_id,
            f.flight_number,
            f.flight_date,
            r.origin_code,
            r.dest_code,
            fs.status_name,
            f.sched_dep_utc,
            f.sched_arr_utc,
            cap.first_name || ' ' || cap.last_name AS captain_name,
            cap.pilot_id AS captain_id,
            fo.first_name || ' ' || fo.last_name AS fo_name,
            fo.pilot_id AS fo_id
        FROM Flight f
        JOIN Route r ON f.route_id = r.route_id
        JOIN FlightStatus fs ON f.status_id = fs.status_id
        LEFT JOIN CrewAssignment ca_cap
            ON ca_cap.flight_id = f.flight_id AND ca_cap.role = 'Captain'
        LEFT JOIN Pilot cap ON cap.pilot_id = ca_cap.pilot_id
        LEFT JOIN CrewAssignment ca_fo
            ON ca_fo.flight_id = f.flight_id AND ca_fo.role = 'First Officer'
        LEFT JOIN Pilot fo ON fo.pilot_id = ca_fo.pilot_id;
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_flight_dest_date
        ON Flight (route_id, flight_date);
        """
    ]
    
    for query in queries:
        cur.execute(query)


def _generate_data(conn: sqlite3.Connection) -> None:
    seed_sql = """
    /* Insert Airports */
    INSERT INTO Airport (airport_code, name, city, country, utc_offset, tz_name) VALUES
    ('LHR','Heathrow','London','United Kingdom', 0.0,'Europe/London'),
    ('LGW','Gatwick','London','United Kingdom', 0.0,'Europe/London'),
    ('MAN','Manchester','Manchester','United Kingdom', 0.0,'Europe/London'),
    ('JFK','John F. Kennedy Intl','New York','United States',-5.0,'America/New_York'),
    ('LAX','Los Angeles Intl','Los Angeles','United States',-8.0,'America/Los_Angeles'),
    ('CDG','Charles de Gaulle','Paris','France', 1.0,'Europe/Paris'),
    ('AMS','Schiphol','Amsterdam','Netherlands',1.0,'Europe/Amsterdam'),
    ('FRA','Frankfurt Main','Frankfurt','Germany',1.0,'Europe/Berlin'),
    ('DXB','Dubai Intl','Dubai','UAE',4.0,'Asia/Dubai'),
    ('SIN','Changi','Singapore','Singapore',8.0,'Asia/Singapore'),
    ('SYD','Kingsford-Smith','Sydney','Australia',10.0,'Australia/Sydney'),
    ('HND','Haneda','Tokyo','Japan',9.0,'Asia/Tokyo'),
    ('YYZ','Pearson','Toronto','Canada',-5.0,'America/Toronto'),
    ('DEL', 'Indira Gandhi Intl', 'Delhi', 'India', 5.5, 'Asia/Kolkata'),
    ('ATL','Hartsfield-Jackson','Atlanta','United States',-5.0,'America/New_York');

    /* Insert Routes */
    INSERT INTO Route (origin_code,dest_code,distance_km,flight_duration_mins) VALUES
    ('LHR','JFK', 5556, 420),
    ('LHR','DXB', 5500, 415),
    ('LGW','AMS',  358,   60),
    ('MAN','CDG',  592,   85),
    ('JFK','LAX', 3974, 330),
    ('CDG','SIN', 10733, 800),
    ('AMS','FRA',  367,   60),
    ('FRA','HND',  9363, 720),
    ('DXB','SYD', 12035, 860),
    ('SIN','SYD',  6300, 480),
    ('HND','LAX',  8821, 650),
    ('YYZ','DEL',   702,  90),
    ('DEL','ATL',   975, 110),
    ('ATL','LHR',  6763, 460),
    ('MAN','YYZ',  5410, 400);

    /* Insert Flight Status */
    INSERT INTO FlightStatus (status_id, status_name) VALUES
    (1, 'Scheduled'),
    (2, 'Boarding'),
    (3, 'Departed'),
    (4, 'Cancelled'),
    (5, 'Delayed');

    /* Insert Pilots */
    INSERT INTO Pilot (licence_no, first_name, last_name, rank, hire_date) VALUES
    ('LIC1001','Alice','Adams','Captain','2015-04-12'),
    ('LIC1002','Bob','Barker','Captain','2012-09-30'),
    ('LIC1003','Cara','Chen','First Officer','2019-06-18'),
    ('LIC1004','Dan','Diaz','First Officer','2020-11-05'),
    ('LIC1005','Eva','Edwards','Captain','2011-02-22'),
    ('LIC1006','Felix','Foley','First Officer','2018-08-14'),
    ('LIC1007','Grace','Gibson','Captain','2010-01-07'),
    ('LIC1008','Hank','Hansen','First Officer','2021-03-28'),
    ('LIC1009','Ivy','Ibrahim','Captain','2014-07-19'),
    ('LIC1010','Jack','Jones','First Officer','2022-10-10'),
    ('LIC1011','Kara','Klein','Captain','2013-05-03'),
    ('LIC1012','Leo','Lopez','First Officer','2017-12-12'),
    ('LIC1013','Mia','Moore','Captain','2009-09-09'),
    ('LIC1014','Ned','Nguyen','First Officer','2023-01-15'),
    ('LIC1015','Ola','Olsen','Captain','2016-06-06');

    /* Insert Flights */
    INSERT INTO Flight (flight_number, flight_date, route_id, sched_dep_utc, sched_arr_utc, status_id) VALUES
    ('BA101','2025-06-05',1,'2025-06-05 08:00','2025-06-05 15:00',1),
    ('BA102','2025-06-06',2,'2025-06-06 07:30','2025-06-06 14:25',1),
    ('BA103','2025-06-07',3,'2025-06-07 09:00','2025-06-07 10:00',1),
    ('BA104','2025-06-08',4,'2025-06-08 10:00','2025-06-08 11:25',2),
    ('BA105','2025-06-09',5,'2025-06-09 12:00','2025-06-09 17:30',1),
    ('BA106','2025-06-10',6,'2025-06-10 06:00','2025-06-10 19:20',1),
    ('BA107','2025-06-11',7,'2025-06-11 08:00','2025-06-11 09:00',1),
    ('BA108','2025-06-12',8,'2025-06-12 03:00','2025-06-12 15:00',1),
    ('BA109','2025-06-13',9,'2025-06-13 00:00','2025-06-13 14:20',1),
    ('BA110','2025-06-14',10,'2025-06-14 02:00','2025-06-14 10:00',2),
    ('BA111','2025-06-15',11,'2025-06-15 18:00','2025-06-16 05:50',1),
    ('BA112','2025-06-16',12,'2025-06-16 13:00','2025-06-16 14:30',1),
    ('BA113','2025-06-17',13,'2025-06-17 15:00','2025-06-17 16:50',1),
    ('BA114','2025-06-18',14,'2025-06-18 09:00','2025-06-18 16:40',5),
    ('BA115','2025-06-19',15,'2025-06-19 07:00','2025-06-19 13:40',3);

    /* Insert Crew Assignments */
    INSERT INTO CrewAssignment (flight_id, pilot_id, role) VALUES
    (1, 1, 'Captain'),
    (2, 2, 'Captain'),
    (3, 5, 'Captain'),
    (4, 7, 'Captain'),
    (5, 9, 'Captain'),
    (6, 11, 'Captain'),
    (7, 13, 'Captain'),
    (8, 15, 'Captain'),
    (9, 1, 'Captain'),
    (10, 2, 'Captain'),
    (11, 5, 'Captain'),
    (12, 7, 'Captain'),
    (13, 9, 'Captain'),
    (14, 11, 'Captain'),
    (15, 13, 'Captain'),
    (1, 3, 'First Officer'),
    (2, 4, 'First Officer'),
    (3, 6, 'First Officer'),
    (4, 8, 'First Officer'),
    (5, 10, 'First Officer');
    """
    
    conn.executescript(seed_sql)