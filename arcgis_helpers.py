import shapely.geometry
import requests
import geopandas as gpd
import pandas as pd






class FeatureLayer:
    def __init__(self, url):
        self.url = url

    # respond with a geodataframe or a dataframe is no geometry is requested
    def query(self, where= '0=0', out_fields = '*', return_geometry = True, intersected_geometry = None):
        # create feature service query url
        query_url = self.url + '/query'
        # chaneg shapely geometry to esrijson   
        if intersected_geometry:
            intersected_geometry = shapely.geometry.mapping(intersected_geometry)
        # create query parameters
        params = {
            'where': where,
            'outFields': out_fields,
            'returnGeometry': return_geometry,
            'f': 'geojson',
            'geometry': intersected_geometry
        }


        # make the request
        response = requests.post(query_url, params = params)
        # parse the response
        data = response.json()
        # loop if the data is paginated
        features = data['features']
        while 'exceededTransferLimit' in data:
            params['resultOffset'] = len(features)
            response = requests.post(query_url, params = params)
            data = response.json()
            features.extend(data['features'])
        # create a geodataframe
        



        if return_geometry:
            gdf = gpd.GeoDataFrame.from_features(features)
            return gdf
        
        else:
            # explode out the geojson properties for the dataframe
            for i, feature in enumerate(features):
                for key, value in feature['properties'].items():
                    features[i][key] = value

                    # remove the properties key
                del features[i]['properties']

      
            df = pd.DataFrame(features)
            return df

  
township_fs_url = 'https://gis.blm.gov/arcgis/rest/services/Cadastral/BLM_Natl_PLSS_CadNSDI/MapServer/1'
township_fs = FeatureLayer(township_fs_url)

section_fs_url = 'https://gis.blm.gov/arcgis/rest/services/Cadastral/BLM_Natl_PLSS_CadNSDI/MapServer/2'
section_fs = FeatureLayer(section_fs_url)

intersected_fs_url = 'https://gis.blm.gov/arcgis/rest/services/Cadastral/BLM_Natl_PLSS_CadNSDI/MapServer/3'
intersected_fs = FeatureLayer(intersected_fs_url)



# connect to blm arcgis server and get data
def get_townshiplist():
    # useing arcgis GIS module to connect to the arcgis server
    fetureds = township_fs.query(where="OBJECTID > 0", out_fields='PLSSID', return_geometry= False)


    return fetureds["PLSSID"].tolist()


def get_township(plssid):
    township_features = township_fs.query(where=f"PLSSID = '{plssid}'")
    return township_features.iloc[0]
