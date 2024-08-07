import os
import geopandas as gpd
import fiona

import preprocessor

# plss-monster dir does not exist
if not os.path.exists('plss-monster'):
    preprocessor.run()






