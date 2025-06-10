"""
Validation functions and custom exceptions for the Flight Management System
"""

from datetime import datetime
from config import (
    VALID_STATUSES, VALID_RANKS, DATE_FORMAT, 
    DATETIME_FORMAT, AIRPORT_CODE_LENGTH
)


class FlightManagementError(Exception):
    pass


class ValidationError(FlightManagementError):
    pass


class DatabaseError(FlightManagementError):
    pass


def validate_date(date_string: str) -> str:
    # Validate that the date string matches the expected format
    try:
        datetime.strptime(date_string, DATE_FORMAT)
        return date_string
    except ValueError:
        raise ValidationError(f"Invalid date format: {date_string}. Use YYYY-MM-DD")


def validate_datetime(datetime_string: str) -> str:
     # Validate that the datetime string matches the expected format from the config
    try:
        datetime.strptime(datetime_string, DATETIME_FORMAT)
        return datetime_string
    except ValueError:
        raise ValidationError(f"Invalid datetime format: {datetime_string}. Use YYYY-MM-DD HH:MM")


def validate_airport_code(code: str) -> str:
    # Validate that the airport code is the correct length and only contains letters
    code = code.upper().strip()
    if len(code) != AIRPORT_CODE_LENGTH or not code.isalpha():
        raise ValidationError(f"Airport code must be exactly {AIRPORT_CODE_LENGTH} letters")
    return code


def validate_flight_status(status: str) -> str:
     # Validate that the flight status is one of the allowed statuses
    status = status.capitalize()
    if status not in VALID_STATUSES:
        raise ValidationError(f"Invalid status. Must be one of: {', '.join(VALID_STATUSES)}")
    return status


def validate_rank(rank: str) -> str:
    # Validate that the rank is one of the allowed ranks
    rank = rank.title()
    if rank not in VALID_RANKS:
        raise ValidationError(f"Rank must be one of: {', '.join(VALID_RANKS)}")
    return rank


def validate_positive_number(value: float, field_name: str) -> float:
    if value <= 0:
        raise ValidationError(f"{field_name} must be positive")
    return value


def validate_flight_number(flight_number: str) -> str:
    # Validate that the flight number is not empty
    flight_number = flight_number.upper().strip()
    if not flight_number:
        raise ValidationError("Flight number cannot be empty")
    return flight_number


def validate_licence_number(licence: str) -> str:
        # Validate that the licence number is not empty 
    licence = licence.strip().upper()
    if not licence:
        raise ValidationError("Licence number cannot be empty")
    return licence