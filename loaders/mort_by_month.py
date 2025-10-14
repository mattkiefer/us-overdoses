"""
get per-100k figure
for every 12-month-ending 
countymonth, statemonth
"""
from models import StateMonth, CountyMonth, StateYear

def update():
    for month in list(CountyMonth.filter()) + list(StateMonth.filter()):
        month.set_mort_data()
        month.save()

    for stateyear in StateYear.filter():
        stateyear.set_od_mort()
        stateyear.save()
