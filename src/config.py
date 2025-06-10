"""
Some constants for the Flight Management System
"""

DB_PATH = "flight_management_test.db"

VALID_STATUSES = {'Scheduled', 'Boarding', 'Departed', 'Cancelled', 'Delayed'}
VALID_RANKS = {'Captain', 'First Officer'}

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M'

AIRPORT_CODE_LENGTH = 3 

MENU_WIDTH = 60
SECTION_SEPARATOR = "=" * MENU_WIDTH