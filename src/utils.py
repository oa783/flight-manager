"""
Functions for formatting and handling user inputs 
"""

from typing import List, Optional, Any
import sqlite3
import pandas as pd


def print_section_header(title: str, width: int = 50):
    print(f"\n{'=' * width}")
    print(f"{title:^{width}}")
    print(f"{'=' * width}\n")

def format_flight_table(flights) -> None:
    """Format and prepare flight data for table display"""

    # If the flight list is empty, error and exit
    if not flights:
        print("No flights found matching the criteria. Sorry!")
        return
    
    flight_data = []
    for flight in flights:
        flight_dict = dict(flight) # Convert each flight object to a dictionary

        # Remove internal identifiers that shouldn't be displayed
        flight_dict.pop('flight_id', None)
        flight_dict.pop('captain_id', None)
        flight_dict.pop('fo_id', None)

        flight_data.append(flight_dict)
    
    if not flight_data: # Exit without warning if data is empty after cleaning
        return
    
    
    
    headers = list(flight_data[0].keys())
    
    # Calculate the maximum width for each column based on the header name and the longest data entry

    col_widths = {}
    for header in headers:
        # maximum width of the data in this column
        max_data_width = max(len(str(row.get(header, ""))) for row in flight_data)
        col_widths[header] = max(len(header), max_data_width)
    

    print("\n")

    # Print the top border line (total width = sum of column widths + padding)
    
    print("-" * (sum(col_widths.values()) + len(headers) * 3 + 1))

    # header row with each header centered within its column width
    header_row = "| "
    for header in headers:
        header_row += f"{header:^{col_widths[header]}} | "
    print(header_row)

    # Print another border line below the headers, same logic as above
    print("-" * (sum(col_widths.values()) + len(headers) * 3 + 1))
    
    # Print data rows
    for row in flight_data:
        data_row = "| "
        for header in headers:
            value = str(row.get(header, ''))

            data_row += f"{value:^{col_widths[header]}} | "
        
        print(data_row)
    
    print("-" * (sum(col_widths.values()) + len(headers) * 3 + 1))
    print(f"\nTotal flights: {len(flights)}")


def format_summary_table(df: pd.DataFrame, title: str) -> None:
    print_section_header(title)
    if df.empty: # If a table has no data...
        print("No data available. Sorry!")
        return
    
    print(df.to_string(index=False)) # Print table as string without the index column



def format_preview(old: dict, new: dict):
    """Format and display a preview of changes

    Args:
        old (dict): The original data
        new (dict): The proposed new data

    Prints diff highlighting any fields that have changed
    """

     # Print the current values
    print("\n--- Current ---")
    for k, v in old.items():
        if k not in ("flight_id",): # Skip flight_id from display
            print(f"{k:<20}: {v}")
    
    # Print the new  values
    print("\n--- Proposed ---")
    changes = [] # List to track the fields that have changed
    for k, v in new.items():
        if k in old and old[k] != v:
            changes.append(k)
            print(f"{k:<20}: {v} [CHANGED]")
        else:
            print(f"{k:<20}: {v}")
    
    # Print changes
    if changes:
        print(f"\nChanges: {', '.join(changes)}")
    else:
        print("No changes detected.")



def safe_input(prompt: str, input_type=str, allow_blank=False):
    """Safely get user input,  ensuring the correct type and handling blank input"""
    while True:
        try:
            value = input(prompt).strip()
            if not value and allow_blank:
                return None
            if not value:
                print("This field cannot be empty.")
                continue
            # Try converting input to the specified type
            if input_type == int:
                return int(value)
            elif input_type == float:
                return float(value)
            return value # Return as string by default
        except ValueError:
            # If the conversion fails notify user to try again
            print(f"Invalid input. Please enter a valid {input_type.__name__}.")


def confirm_action(prompt: str = "Apply changes? (y/n): ") -> bool:
    return input(prompt).strip().lower() == "y"