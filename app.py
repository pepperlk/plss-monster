import os
import geopandas as gpd
import fiona

from preprocessor import run
from township_processor import stage_townships, process_townships, stage_storage

# plss-monster dir does not exist
if not os.path.exists('plss-monster'):
    run()
    # preprocessor.run()
    # stage_townships()

    stage_storage()
    
    # all 50 states short codes
    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
              'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
              'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
              'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
              'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']


    states = ['WY']

    for state in states:
        process_townships(state)
    # process_townships()










