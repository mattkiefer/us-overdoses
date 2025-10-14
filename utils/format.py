from datetime import datetime

def round_float(x,decimals=2):
    """
    improves readibility of rates and percents
    with configurable precision level
    """
    return round(float(x),decimals) if x is not None else None

def year_month_str(year,month_name):
    """
    four-digit year, spelled out month
    returns YYYY-MM-DD
    using first of the month placeholder
    """
    # Parse month name to month number using datetime.strptime
    month_number = datetime.strptime(month_name, '%B').month
    # Format year-month-day string with day fixed as 01
    return f"{year:04d}-{month_number:02d}-01"

def month_name_to_number(name):
    return list(__import__('calendar').month_name).index(name)
