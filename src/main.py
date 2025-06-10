"""
Flight Management System
Course: CM500292 - Databases Coursework
"""

from ui import main_menu

# Flight Management System entry point
def main():
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\nGoodbye - Enjoy all future flights :) ")
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    main()