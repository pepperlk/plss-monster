import math
import os
from time import sleep, time
import geopandas as gpd
import fiona
import pandas as pd
from shapely import Polygon, MultiPolygon
from tqdm import tqdm
from pytictoc import TicToc
from subdivide import irregular_subdivision, subdivide_polygon, find_cornerpoints, bearing, distance
t = TicToc()

skip_valid = False

def run():
    print('Running preprocessor...')

    if os.path.exists('plss-monster'):
        print('PLSS data already loaded.')
        return

   

    plss_link = 'https://www.arcgis.com/sharing/rest/content/items/283939812bc34c11bad695a1c8152faf/data'

    # if plss.gdb is not in the current directory, download it
    if not os.path.exists('plss.gdb'):
        print('PLSS data not found in current directory.')
        # download the file if not already downloaded
        if not os.path.exists('plss.zip'):
            print('Downloading PLSS data...')
            os.system(f'wget {plss_link} -O plss.zip')
        if os.path.exists('plss.zip') and not os.path.exists('plss.gdb'):
            print('Unzipping PLSS data...')
            os.system('unzip plss.zip')
            # rename directory *plss.gdb to plss.gdb
            os.system('mv mv *plss.gdb plss.gdb')
            # remove the *_State_*.gdb directory
            os.system('rm -r *_State_*.gdb')
            # remove the zip file
            os.system('rm plss.zip')


    # fiona list layers in the PLSS data
    layers = fiona.listlayers('plss.gdb')

    # Print the list of layers
    print("Layers in the file geodatabase:")
    for layer in layers:
        print(layer)



    # use fiona to read layer 0 and get al the state attributes in the PLSS data
    # states = []
    # with fiona.open('plss.gdb', layer='PLSSTownship', where="STATEABBR = 'WY'") as src:
        
    #     for feature in src:
    #         state = feature['properties']['STATEABBR']
    #         if state not in states:
    #             states.append(state)



    # us_states_abbr =['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

    us_states_abbr = ['WY']
    if not os.path.exists('plss-monster_tmp'):
        os.makedirs('plss-monster_tmp')

    return



    for state in tqdm(us_states_abbr):
        state_dir = f'plss-monster_tmp/{state}'
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)
        # using ogr2ogr to convert the PLSS data to flatgeobuf using the state abbreviation
        if not os.path.exists(f'plss-monster_tmp/{state}/townships.fgb'):
            # print(f'Converting PLSS data for {state} to flatgeobuf...')
            os.system(f'ogr2ogr -f "FlatGeobuf" plss-monster_tmp/{state}/townships.fgb plss.gdb -sql "SELECT * FROM PLSSTownship WHERE STATEABBR = \'{state}\'" -skipfailures')
        # else:
            # print(f'PLSS data for {state} already converted to flatgeobuf.')




    # build out state directories and save the townships
    for state in us_states_abbr:
        print(f'Loading Township data for {state}...')
        state_dir = f'plss-monster_tmp/{state}'
        plss = gpd.read_file(f'{state_dir}/townships.fgb', driver='FlatGeobuf')


# DEBUG!!!!
        # # for each township, create a directory 
        # for township in tqdm(plss['PLSSID']):
        #     township_dir = f'{state_dir}/{township}'
        #     if not os.path.exists(township_dir):
        #         os.makedirs(township_dir)
       




    def section_valid(geom):
        # get centroid of the section bounds by creating a new polygon and getting its centroid

        # bound are xmin, ymin, xmax, ymax
        # create a polygon from the bounds



        


        
        nw_point, ne_point, se_point, sw_point = find_cornerpoints(geom)

        if nw_point is None or ne_point is None or se_point is None or sw_point is None:
            return False, None

        



        # create a new polygon from the points
        new_poly = Polygon([(nw_point.x, nw_point.y), (ne_point.x, ne_point.y), (se_point.x, se_point.y), (sw_point.x, sw_point.y)])
               
        north_distance = distance(nw_point, ne_point)
        east_distance = distance(ne_point, se_point)
        south_distance = distance(se_point, sw_point)
        west_distance = distance(sw_point, nw_point)
        
        # get avg and std deviation of the distances
        avg_distance = (north_distance + east_distance + south_distance + west_distance) / 4
        # std deviation of side lengths
        std_dev = math.sqrt(((north_distance - avg_distance) ** 2 + (east_distance - avg_distance) ** 2 + (south_distance - avg_distance) ** 2 + (west_distance - avg_distance) ** 2) / 4)
        
        if std_dev < 40: # and close(avg_distance, 2100, 400):
            return True, new_poly

        

        return False, new_poly

        


    debug_townships = None #["WY060330N1000W0", "WY060330N0990W0", "WY060340N0980W0", "WY060320N0990W0", "WY340020S0020E0", "WY060340N1000W0" ]



    state_sections = None

    for state in us_states_abbr:

        where = f"PLSSID like '{state}%'"
        if debug_townships:
            where = f"PLSSID in ('{'\', \''.join(debug_townships)}')"
        # process down the sections and the intersected of the state
        print(f'Processing sections for {state}...')
        state_dir = f'plss-monster_tmp/{state}'
        if not os.path.exists(f'{state_dir}/state_sections.fgb'):
            # use ogr2ogr to convert the PLSS data to flatgeobuf using the state abbreviation
            print(f'Converting PLSS data for {state} to flatgeobuf...')
            os.system(f'ogr2ogr -f "FlatGeobuf" {state_dir}/state_sections.fgb plss.gdb -sql "SELECT * FROM PLSSFirstDivision WHERE {where}" -skipfailures')
        # process intersected
        if not os.path.exists(f'{state_dir}/state_intersected.fgb'):
            print(f'Converting intersected data for {state} to flatgeobuf...')
            os.system(f'ogr2ogr -f "FlatGeobuf" {state_dir}/state_intersected.fgb plss.gdb -sql "SELECT * FROM PLSSIntersected WHERE {where}" -skipfailures')

    # process the sections for the next township and save and exit

    print('Processing sections... Load GDF')
    # premark sections as 1 mile by 1 mile square as valid_aliquot sections
    gdf = gpd.read_file(f'{state_dir}/state_sections.fgb', driver='FlatGeobuf')
    # add a column to the geodataframe
    gdf['valid_aliquot'] = False
    # loop the sections
    print('Processing sections... Looping')
    # get length of the geodataframe
    total_sections = len(gdf)
    pbar = tqdm(total_sections)
    debug_sections = []
    if not skip_valid:
        for index, row in gdf.iterrows():
            # get section geometry
            geom = row['geometry']
            # get corner points
            valid, section_out_poly = section_valid(geom)
            gdf.loc[index, 'valid_aliquot'] = valid
            debug_sections.append({'valid': valid, 'section_out': section_out_poly})
            pbar.update(1)



        debug_sections_gdf = gpd.GeoDataFrame(debug_sections, geometry='section_out', crs=gdf.crs)

        # rmeove null geometries
        debug_sections_gdf = debug_sections_gdf[debug_sections_gdf['section_out'].notnull()]

        debug_sections_gdf.to_file(f'{state_dir}/debug_sections.fgb', driver='FlatGeobuf')


    print('Processing sections... Save fgb')
    # save the geodataframe
    gdf.to_file(f'{state_dir}/state_sections.fgb', driver='FlatGeobuf')


    if not skip_valid:
        # subdivide valid sections
        valid_sections = gdf[gdf['valid_aliquot'] == True]
        debug_second_divisions = []
        debug_third_divisions = []
        debug_fourth_divisions = []
        debug_fifth_divisions = []
        for index, row in valid_sections.iterrows():
            geom = row['geometry']
            subdivided = subdivide_polygon(geom)
            # loop and append the debug polygons
            for debug_polygon in subdivided:
                debug_second_divisions.append({'processed': False, 'geometry': debug_polygon})
                # get the third divisions
                subdivided_third = subdivide_polygon(debug_polygon)
                for debug_polygon_third in subdivided_third:
                    debug_third_divisions.append({'processed': False, 'geometry': debug_polygon_third})
                    # get the fourth divisions
                    subdivided_fourth = subdivide_polygon(debug_polygon_third)
                    # for debug_polygon_fourth in subdivided_fourth:
                    #     debug_fourth_divisions.append({'processed': False, 'geometry': debug_polygon_fourth})
                    #     # get the fifth divisions
                    #     subdivided_fifth = subdivide_polygon(debug_polygon_fourth)
                    #     for debug_polygon_fifth in subdivided_fifth:
                    #         debug_fifth_divisions.append({'processed': False, 'geometry': debug_polygon_fifth})
        

        debug_second_divisions_gdf = gpd.GeoDataFrame(debug_second_divisions, geometry='geometry', crs=gdf.crs)
        debug_second_divisions_gdf.to_file(f'{state_dir}/debug_second_divisions.fgb', driver='FlatGeobuf')

        debug_third_divisions_gdf = gpd.GeoDataFrame(debug_third_divisions, geometry='geometry', crs=gdf.crs)
        debug_third_divisions_gdf.to_file(f'{state_dir}/debug_third_divisions.fgb', driver='FlatGeobuf')

        # debug_fourth_divisions_gdf = gpd.GeoDataFrame(debug_fourth_divisions, geometry='geometry', crs=gdf.crs)
        # debug_fourth_divisions_gdf.to_file(f'{state_dir}/debug_fourth_divisions.fgb', driver='FlatGeobuf')

        # debug_fifth_divisions_gdf = gpd.GeoDataFrame(debug_fifth_divisions, geometry='geometry', crs=gdf.crs)
        # debug_fifth_divisions_gdf.to_file(f'{state_dir}/debug_fifth_divisions.fgb', driver='FlatGeobuf')



    # invalid sections
    invalid_sections = gdf[gdf['valid_aliquot'] == False]
    debug_invalid_sections = []
    for index, row in invalid_sections.iterrows():
        geom = row['geometry']
        # if "WY060340N1000W0SN280" == row['FRSTDIVID']:

        # subdivide the invalid section
        subdivided = irregular_subdivision(geom, 2200/2.0)
        for debug_polygon in subdivided:
            debug_invalid_sections.append({'processed': False, 'geometry': debug_polygon})

    debug_invalid_sections_gdf = gpd.GeoDataFrame(debug_invalid_sections, geometry='geometry', crs=gdf.crs)
    debug_invalid_sections_gdf.to_file(f'{state_dir}/estimated_second_division.fgb', driver='FlatGeobuf')



    return






    
                
                

