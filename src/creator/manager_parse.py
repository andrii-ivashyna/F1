# manager_parse.py
"""
Manager for all parsing operations.
Coordinates parsing from Wikipedia and F1.com for circuits, teams, and drivers.
"""

from config import log

# Import parsing functions from individual modules
from parse_circuit import parse_circuit_wiki, parse_circuit_f1
from parse_team import parse_team_wiki, parse_team_f1
from parse_driver import parse_driver_f1

def run_all_parsers():
    """Runs all parsing operations in sequence."""
    log("Enriching data from external sources", 'SUBHEADING')
    run_circuit_parsers()
    run_team_parsers()
    run_driver_parsers()

def run_circuit_parsers():
    """Runs all circuit-related parsers."""
    parse_circuit_wiki()
    parse_circuit_f1()

def run_team_parsers():
    """Runs all team-related parsers."""
    parse_team_wiki()
    parse_team_f1()

def run_driver_parsers():
    """Runs all driver-related parsers."""
    parse_driver_f1()

# Individual parser functions for granular control
def parse_circuit_wiki():
    """Parse circuit data from Wikipedia."""
    from parse_circuit import parse_circuit_wiki as _parse_circuit_wiki
    _parse_circuit_wiki()

def parse_circuit_f1():
    """Parse circuit data from F1.com."""
    from parse_circuit import parse_circuit_f1 as _parse_circuit_f1
    _parse_circuit_f1()

def parse_team_wiki():
    """Parse team data from Wikipedia."""
    from parse_team import parse_team_wiki as _parse_team_wiki
    _parse_team_wiki()

def parse_team_f1():
    """Parse team data from F1.com."""
    from parse_team import parse_team_f1 as _parse_team_f1
    _parse_team_f1()

def parse_driver_f1():
    """Parse driver data from F1.com."""
    from parse_driver import parse_driver_f1 as _parse_driver_f1
    _parse_driver_f1()
