"""
CLI User interface for the Flight Management System
"""

from database import initialise_tables
from config import MENU_WIDTH
from utils import (
    print_section_header, safe_input, format_flight_table
)

from queries import (
    get_flights, change_route, change_times, change_status,
    change_captain, change_first_officer
)
from queries_add import (
    add_airport, add_route, add_pilot, add_flight, assign_captain,
    flights_per_destination, flights_per_destination_date_range,
    flights_per_pilot, flights_by_status, top_busiest_routes,
    show_all_pilots, show_all_airports, show_all_routes,
    show_pilots_by_rank
)


def menu_view_flights():
    """Request user input to select flight by filter""" 
    print_section_header("Filter Flights", MENU_WIDTH)
    print("Leave blank to skip any filter\n")
    
    # get filter criteria from user
    origin = safe_input("Origin code: ",allow_blank=True)
    dest = safe_input("Destination code: ",allow_blank=True)
    status = safe_input("Status (Scheduled/Boarding/Departed/Cancelled/Delayed): ", allow_blank=True)
    date_from = safe_input("Date from (YYYY-MM-DD): ",allow_blank=True)
    date_to = safe_input("Date to (YYYY-MM-DD): ",  allow_blank=True)
    captain_id = safe_input("Captain ID: ", int ,allow_blank=True)
    
    # Query the database for flights matching the provided filters
    flights = get_flights(origin, dest, status, date_from, date_to, captain_id)

    # Show results
    format_flight_table(flights)


def menu_modify_flight():
    """Request user input to modify a flight"""
    print_section_header("Modify Flight", MENU_WIDTH)
    
     # Identify the flight to modify. Flight number is not unique but (Flight number, flight date) is unique
    flight_number = safe_input("Flight number: ")
    flight_date = safe_input("Flight date (YYYY-MM-DD): ")
    
    # Present modification options to the user
    print("""
    1) Change departure/arrival times
    2) Change status
    3) Change route (origin & destination)
    4) Reassign captain
    5) Reassign first officer
    0) Back to main menu
    """)
    
    choice = safe_input("Choice (0-5): ")
    
    if choice == "1":
        new_dep = safe_input("New departure UTC (YYYY-MM-DD HH:MM): ") 
        new_arr = safe_input("New arrival UTC (YYYY-MM-DD HH:MM): ")  
        change_times(flight_number, flight_date, new_dep, new_arr)
        
    elif choice == "2":
        new_status = safe_input("New status (Scheduled/Boarding/Departed/Cancelled/Delayed): ") 
        change_status(flight_number, flight_date, new_status) 
        
    elif choice == "3": 
        new_origin = safe_input("New origin code: ") 
        new_dest = safe_input("New destination code: ") 
        change_route(flight_number, flight_date, new_origin, new_dest) 
         
    elif choice == "4":
        print("Available Captains:") 
        show_pilots_by_rank("Captain") 
        new_captain_id = safe_input("New captain pilot ID: ", int) 
        change_captain(flight_number, flight_date, new_captain_id)
        
    elif choice == "5":  
        print("Available First Officers:")
        show_pilots_by_rank("First Officer") 
        new_fo_id = safe_input("New first officer pilot ID: ", int)
        change_first_officer(flight_number, flight_date, new_fo_id)


def menu_add_data():
    """Prompt user to add new airports, routes, pilots, flights or assign captains"""
    
    print_section_header("Add New Data", MENU_WIDTH)
    print("""
    1) New airport
    2) New Route 
    3) New pilot
    4) New flight
    5) Assign captain to flight
    0) Back to main menu
    """)
    
    choice = safe_input("Choice (0-5): ")
    
    if choice == "1":
        add_airport(
            safe_input("Airport code (3 letters): "),
            safe_input("Airport name: "),
            safe_input("City: "),
            safe_input("Country: "),
            safe_input("UTC offset (e.g., -5, 0, 5.5): ", float),
            safe_input("Timezone name (e.g., Europe/London): ")
        )
        
    elif choice == "2":
        print("Available airports:")
        show_all_airports()
        print('\n')
        add_route(
            
            safe_input("Origin code: "),
            safe_input("Destination code: "),
            safe_input("Distance (km): ", float),
            safe_input("Flight duration (minutes): ", int)
        )
        
    elif choice == "3":
        add_pilot(
            safe_input("Licence number: "),
            safe_input("First name: "),
            safe_input("Last name: "),
            safe_input("Rank (Captain/First Officer): "),
            safe_input("Hire date (YYYY-MM-DD): ")
        )
        
    elif choice == "4":
        print("To add a flight, you need a route ID. Here are existing routes:")
        show_all_routes()
        print()
        add_flight(
            safe_input("Flight number: "),
            safe_input("Flight date (YYYY-MM-DD): "),
            safe_input("Route ID: ", int),
            safe_input("Departure UTC (YYYY-MM-DD HH:MM): "),
            safe_input("Arrival UTC (YYYY-MM-DD HH:MM): "),
            safe_input("Status [Scheduled]: ", allow_blank=True) or "Scheduled"
        )
        
    elif choice == "5":
        assign_captain(
            safe_input("Flight ID: ", int),
            safe_input("Captain pilot ID: ", int)
        )


def menu_summaries():
    """Show summary reports"""
    print_section_header("Summary Reports", MENU_WIDTH)
    print("""
    1) Flights per destination
    2) Flights per destination (date range)
    3) Flights per pilot
    4) Flights by status
    5) Top busiest routes
    6) All pilots
    7) All airports
    0) Back to main menu
    """)
    
    choice = safe_input("Choice (0-7): ")
    
    if choice == "1":
        flights_per_destination()
    elif choice == "2":
        date_from = safe_input("From date (YYYY-MM-DD): ")
        date_to = safe_input("To date (YYYY-MM-DD): ")
        flights_per_destination_date_range(date_from, date_to)
    elif choice == "3":
        flights_per_pilot()
    elif choice == "4":
        flights_by_status()
    elif choice == "5":
        limit = safe_input("Number of routes to show [10]: ", int, allow_blank=True) or 10
        top_busiest_routes(limit)
    elif choice == "6":
        show_all_pilots()
    elif choice == "7":
        show_all_airports()


def main_menu():
    """Entry point for the CLI — initialise the database and display main menu"""
    
    # Print borders for styling + improved user experience
    print("\n" + "=" * MENU_WIDTH)
    print(f"{'Flight Management System':^{MENU_WIDTH}}")
    print("=" * MENU_WIDTH + "\n")
    
    try:
        # Initalise the database
        initialise_tables()
    except Exception as e:
        print(f"Failed to initialise the database: {e}")
        return
    
    # Main interaction loop
    while True:
        print_section_header("Main Menu", MENU_WIDTH)
        print("""
1) View/Filter flights
2) Modify flight
3) Add new data
4) Summary reports
0) Exit
        """)
        
        choice = safe_input("Select option (0-4): ")
        
        try:
            if choice == "1":
                menu_view_flights()
            elif choice == "2":
                menu_modify_flight()
            elif choice == "3":
                menu_add_data()
            elif choice == "4":
                menu_summaries()
            elif choice == "0":
                print("Thank you for using the Flight Management System")
                print("Have a great day")
                break
            else:
                print("INVALID, Please try again.")
        except KeyboardInterrupt:
            print("\nOperation cancelled...")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")