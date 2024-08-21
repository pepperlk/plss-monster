
import os
import fiona
import pandas as pd
import geopandas as gpd
from shapely import MultiPolygon
from tqdm import tqdm
from subdivide import section_valid, subdivide_polygon, irregular_subdivision
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
# import tictoc
from pytictoc import TicToc
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from geoalchemy2.shape import from_shape
from concurrent.futures import ThreadPoolExecutor



# import ogr

plss_townships = None
plss_first_divisions = None
plss_intersections = None


def column_exists(cursor, table_name, column_name):
    query = sql.SQL(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_name = %s;"
    )
    cursor.execute(query, (table_name, column_name))
    return cursor.fetchone() is not None


def check_tablefor_columns(table, columns):
    conn = psycopg2.connect(os.getenv('DATABASE_CS'))
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
    # check if the columns exist in the table
    for column in columns:
        if not column_exists(cur, table, column):
            # add the column to the table
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {columns[column]}")
    conn.commit()
    cur.close()

  
    pass

def table_exists(table_name):
    conn = psycopg2.connect(os.getenv('DATABASE_CS'))
    cursor = conn.cursor()
    query = sql.SQL(
        "SELECT table_name FROM information_schema.tables WHERE table_name = %s;"
    )
    cursor.execute(query, (table_name,))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists


def stage_storage():


    # connect to the postgis database using ENV DATABASE_URL and check for the existence of the plss tables
    # if it exists, check for the existence of the Townships, FirstDivisions, Intersections tables
    db_cs = os.getenv('DATABASE_CS')
    if db_cs is None:
        raise Exception("DATABASE_CS environment variable is not set")
    
    # parse the connection string to a dictionary
    db_cs_dict = dict([part.split('=') for part in db_cs.split()])
    if 'port' not in db_cs_dict:
        db_cs_dict['port'] = '5432'

    # convert old style connection string to new style url
    db_url = "postgresql://"
    db_url += db_cs_dict['user'] + ":"
    db_url += db_cs_dict['password'] + "@"
    db_url += db_cs_dict['host'] + ":"
    db_url += db_cs_dict['port'] + "/"
    db_url += db_cs_dict['dbname']
    # remove ' from the string
    db_url = db_url.replace("'", "")

    print(db_url)
    # set env variable DATABASE_URL to the new style connection string
    os.environ['DATABASE_URL'] = db_url
    
    
    
    
    # check using table_exists function and check if the table exists in the database
    process_townships = not table_exists('plsstownship')
    if process_townships:
        print("Townships not in PG database")
   
    


    process_sections = not table_exists('plssfirstdivision')
    if process_sections:
        print("FirstDivisions not in PG database")

    process_intersections = not table_exists('plssintersected')
    if process_intersections:
        print("Intersections not in PG database")


    process_qsec = not table_exists('plssqsec')
    if process_qsec:
        print("Intersections not in PG database")
 

    if process_townships:
        # create the Townships table in the postgis database
        os.system(f'ogr2ogr -f "PostgreSQL" PG:"{db_cs}" plss.gdb PLSSTownship -nln PLSSTownship')
        check_tablefor_columns('plsstownship', {'processed':"INTEGER", 'sections': "INTEGER", 'second_divisions': "INTEGER", 'intersected': "INTEGER", 'valid_sections': "INTEGER", 'intersected_modified': "INTEGER"})

    if process_sections:
        # create the FirstDivisions table in the postgis database
        os.system(f'ogr2ogr -f "PostgreSQL" PG:"{db_cs}" plss.gdb PLSSFirstDivision -nln PLSSFirstDivision')
        check_tablefor_columns('plssfirstdivision', {'valid':"INTEGER"})

    if process_intersections:
        # create the Intersections table in the postgis database
        os.system(f'ogr2ogr -f "PostgreSQL" PG:"{db_cs}" plss.gdb PLSSIntersected -nln PLSSIntersected')
        check_tablefor_columns('plssintersected', {'modified':"INTEGER"})


    if process_qsec:
        # copy the plssfirstdivision schema to the plssqsec schema
        conn = psycopg2.connect(os.getenv('DATABASE_CS'))
        cur = conn.cursor()
        # CREATE TABLE {new_table_name} (LIKE {existing_table_name} INCLUDING ALL);
        cur.execute("CREATE TABLE plssqsec (LIKE plssfirstdivision INCLUDING ALL);")
        conn.commit()
        check_tablefor_columns('plssqsec', {'QSEC':"TEXT"})
    

    # check for processing columns in the Townships table
    # if it does not exist, create the columns






    
    
    
    








def stage_townships():
    # Load the townships data
    townships = []

    if os.path.exists('plss-monster_tmp/townships.csv'):
        return

    # using fiona open the plss.gdb Townships layer
    with fiona.open('plss.gdb', layer='PLSSTownship') as src:
        # getn count of features in the Townships layer
        # print(len(src))
        for feature in tqdm(src):
            # Append the feature to the townships list
            plssid = feature['properties']['PLSSID']
            townships.append({"id" :plssid, "processed": False})    

    # Save the townships list to a csv file
    df = pd.DataFrame(townships)
    df.to_csv('plss-monster_tmp/townships.csv', index=False)

    
    return townships


def process_qqsec(intersected_gdf,geometry, suffix):
        buffer = 250
        polygon_nw, polygon_ne, polygon_se, polygon_sw = subdivide_polygon(geometry)

        # set the QSEC column to NW where polygon_nw intersects the intersected_gdf geometry and the QSEC is null
        intersected_gdf.loc[(intersected_gdf.within(polygon_nw.buffer(buffer))) & (intersected_gdf['qsec'].isnull()), 'qsec'] = suffix
        intersected_gdf.loc[(intersected_gdf.within(polygon_nw.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'modified'] = 1
        intersected_gdf.loc[(intersected_gdf.within(polygon_nw.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'update'] = True
        intersected_gdf.loc[(intersected_gdf.within(polygon_nw.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'qqsec'] = 'NW' + suffix

        intersected_gdf.loc[(intersected_gdf.within(polygon_ne.buffer(buffer))) & (intersected_gdf['qsec'].isnull()), 'qsec'] = suffix
        intersected_gdf.loc[(intersected_gdf.within(polygon_ne.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'modified'] = 1
        intersected_gdf.loc[(intersected_gdf.within(polygon_ne.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'update'] = True
        intersected_gdf.loc[(intersected_gdf.within(polygon_ne.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'qqsec'] = 'NE' + suffix

        intersected_gdf.loc[(intersected_gdf.within(polygon_se.buffer(buffer))) & (intersected_gdf['qsec'].isnull()), 'qsec'] = suffix
        intersected_gdf.loc[(intersected_gdf.within(polygon_se.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'modified'] = 1
        intersected_gdf.loc[(intersected_gdf.within(polygon_se.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'update'] = True
        intersected_gdf.loc[(intersected_gdf.within(polygon_se.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'qqsec'] = 'SE' + suffix

        intersected_gdf.loc[(intersected_gdf.within(polygon_sw.buffer(buffer))) & (intersected_gdf['qsec'].isnull()), 'qsec'] = suffix
        intersected_gdf.loc[(intersected_gdf.within(polygon_sw.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'modified'] = 1
        intersected_gdf.loc[(intersected_gdf.within(polygon_sw.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'update'] = True
        intersected_gdf.loc[(intersected_gdf.within(polygon_sw.buffer(buffer))) & (intersected_gdf['qqsec'].isnull()), 'qqsec'] = 'SW' + suffix


def process_sections(state, township, cur, conn, pbar, crs):
        
        sections = gpd.read_postgis(f"SELECT shape as geom, * FROM plssfirstdivision WHERE plssid = '{township['plssid']}'", os.getenv('DATABASE_URL'))

        intersected = gpd.read_postgis(f"SELECT shape as geom, * FROM plssintersected WHERE plssid = '{township['plssid']}'", os.getenv('DATABASE_URL'))


        # create state folder if it does not exist
        if not os.path.exists(f'plss-monster_tmp/{state}'):
            os.makedirs(f'plss-monster_tmp/{state}')

        # create the township folder if it does not exist
        if not os.path.exists(f'plss-monster_tmp/{state}/{township["plssid"]}'):
            os.makedirs(f'plss-monster_tmp/{state}/{township["plssid"]}')

        # all the intersected rows have the QSEC column set then early return
        if intersected['qqsec'].notnull().all() & intersected['qsec'].notnull().all():
            pbar.update(1)
            return
        

        # add update column to the intersected dataframe
        intersected['update'] = False


        # loop through the sections
        second_divisions = []
        for index, section in sections.iterrows():
            # check if the section is valid

            # check if the section valid is not null from the rowdata
            valid = section["valid"]
            if valid is None:

                valid, poly,  = section_valid(section['geom'])
            # check if the section is already subdivided
            if valid == True:
                # check if subdivisions already exist
          
                
                # subdivide the section
                nw, ne, se, sw = subdivide_polygon(section['geom'])
                # create the second divisions by appending the first div dictioanry with the new geometry and the qsec
                sectiondict = section.to_dict()
                sectiondict['shape'] = MultiPolygon([nw])
                sectiondict['qsec'] = 'NW'
                second_divisions.append(sectiondict)
                process_qqsec(intersected, nw, "NW")


                sectiondict = section.to_dict()
                sectiondict['shape'] = MultiPolygon([ne])
                sectiondict['qsec'] = 'NE'
                second_divisions.append(sectiondict)
                process_qqsec(intersected, ne, "NE")

                sectiondict = section.to_dict()
                sectiondict['shape'] = MultiPolygon([se])
                sectiondict['qsec'] = 'SE'
                second_divisions.append(sectiondict)
                process_qqsec(intersected, se, "SE")

                sectiondict = section.to_dict()
                sectiondict['shape'] = MultiPolygon([sw])
                sectiondict['qsec'] = 'SW'
                second_divisions.append(sectiondict)
                process_qqsec(intersected, sw, "SW")

                # update the valid column to 1
                cur.execute("UPDATE plssfirstdivision SET valid = 1 WHERE frstdivid = %s", (section['frstdivid'],))
                conn.commit()

            else:
                try:
                    nw, ne, se, sw = irregular_subdivision(section['geom'], 2150/2)
                    # create the second divisions by appending the first div dictioanry with the new geometry and the qsec
                    sectiondict = section.to_dict()
                    sectiondict['shape'] = MultiPolygon([nw])
                    sectiondict['qsec'] = 'NW'
                    second_divisions.append(sectiondict)
                    process_qqsec(intersected, nw, "NW")

                    sectiondict = section.to_dict()
                    sectiondict['shape'] = MultiPolygon([ne])
                    sectiondict['qsec'] = 'NE'
                    second_divisions.append(sectiondict)
                    process_qqsec(intersected, ne, "NE")

                    sectiondict = section.to_dict()
                    sectiondict['shape'] = MultiPolygon([se])
                    sectiondict['qsec'] = 'SE'
                    second_divisions.append(sectiondict)
                    process_qqsec(intersected, se, "SE")

                    sectiondict = section.to_dict()
                    sectiondict['shape'] = MultiPolygon([sw])
                    sectiondict['qsec'] = 'SW'
                    second_divisions.append(sectiondict)
                    process_qqsec(intersected, sw, "SW")

                    # update the valid column to 0
                    cur.execute("UPDATE plssfirstdivision SET valid = 0 WHERE frstdivid = %s", (section['frstdivid'],))
                    conn.commit()
                except Exception as e:
                    pass


        # get all the update rows from the intersected dataframe
        update_rows = intersected[intersected['update'] == True]
        # drop the update column from the intersected dataframe
        update_rows = update_rows.drop(columns=['update'])

        # update the intersected rows in the postgis database
        for index, row in update_rows.iterrows():
            cur.execute("UPDATE plssintersected SET qqsec = %s, qsec = %s, modified = 1 WHERE secdivid = %s", (row['qqsec'], row['qsec'], row['secdivid']))


        if len(sections) > 0:
            # write the sections to a FlatGeoBuf in the township folder
            sections.to_file(f'plss-monster_tmp/{state}/{township["plssid"]}/sections.fgb', driver='FlatGeobuf')

        if len(intersected) > 0:
            # wirite intersected to FlatGeoBuf in the township fodler
            intersected.to_file(f'plss-monster_tmp/{state}/{township["plssid"]}/intersected.fgb', driver='FlatGeobuf')


        conn.commit()

        if len(second_divisions) > 0:
            # create a geodataframe from the second_divisions list
            second_divisions_gdf = gpd.GeoDataFrame(second_divisions, crs=crs, geometry='shape')

            # write the second_divisions_gdf to a FlatGeoBuf in the township folder
            second_divisions_gdf.to_file(f'plss-monster_tmp/{state}/{township["plssid"]}/second_divisions.fgb', driver='FlatGeobuf')

            # rename geom to shape
            # remove geom column
            second_divisions_gdf = second_divisions_gdf.drop(columns=['geom'])

            # is qsec already has the FISTDIVID and the QSEC mates remove them from the postgis database
            cur.execute("DELETE FROM plssqsec WHERE frstdivid = %s", (section['frstdivid'],))
            conn.commit()

            # loop through the second_divisions_gdf and insert the data into the plssqsec table
            for index, second_division in second_divisions_gdf.iterrows():
                # insert the second division into the plssqsec table
                cur.execute("INSERT INTO plssqsec (frstdivid, shape, qsec) VALUES (%s, ST_GeomFromText(%s), %s)", (second_division['frstdivid'], second_division['shape'].wkt, second_division['qsec']))

            conn.commit()

        pbar.update(1)


def process_townships(state):
    # load townships from postgis using geopandas
    townships = gpd.read_postgis(f"SELECT shape as geom, * FROM plsstownship WHERE ( processed = 0 or processed is NULL ) AND stateabbr = '{state}'", os.getenv('DATABASE_URL'))




    pbar = tqdm(total=len(townships))

    conn = psycopg2.connect(os.getenv('DATABASE_CS'))
    cur = conn.cursor()

    # create indexes on the plssfirstdivision table for frstdivid and plssid
    cur.execute("CREATE INDEX IF NOT EXISTS frstdivid_idx ON plssfirstdivision (frstdivid);")
    cur.execute("CREATE INDEX IF NOT EXISTS plssid_idx ON plssfirstdivision (plssid);")

    # create indexes on the townships table for plssid
    cur.execute("CREATE INDEX IF NOT EXISTS plssid_idx ON plsstownship (plssid);")

    # create indexes on the plssintersected table for frstdivid and plssid and secdivid
    cur.execute("CREATE INDEX IF NOT EXISTS frstdivid_idx ON plssintersected (frstdivid);")
    cur.execute("CREATE INDEX IF NOT EXISTS plssid_idx ON plssintersected (plssid);")
    cur.execute("CREATE INDEX IF NOT EXISTS secdivid_idx ON plssintersected (secdivid);")

    conn.commit()


    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for index, township in townships.iterrows():
            # get sections
            future = executor.submit(process_sections, state, township, cur, conn, pbar, townships.crs)
            futures.append(future)
            

        for future in futures:
            future.result()

    cur.close()
    conn.close()

      
     




        
        
     

    cur.close()
    conn.close()
        






   

    


   

  