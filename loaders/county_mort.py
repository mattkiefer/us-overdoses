"""
just runs derivations
"""

from models import County, db

def update():
    counties = []
    for county in County.filter():
        county.set_mort_100k()
        counties.append(county)

    with db.atomic():
        County.bulk_update(counties,fields=['mort_per_100k_202412'],batch_size=500)
