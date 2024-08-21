import math
import pandas as pd
from shapely import Polygon, MultiPolygon, LineString, Point

def close(value, target, tolerance=0.001):
    return abs(value - target) < tolerance  

# bearing using north as 0 degrees with 0-360 degrees clockwise
def bearing(p1, p2):
    angle = math.degrees(math.atan2(p2.x - p1.x, p2.y - p1.y))
    if angle < 0:
        angle += 360
    return angle


def project_point(p1, angle, distance):
    angle = math.radians(angle)
    x = p1.x + distance * math.sin(angle)
    y = p1.y + distance * math.cos(angle)
    return Point(x, y)


def distance(p1, p2):
    return math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)

# get the angle of 3 points
def angle(p1, p2, p3):
    # p1 to p2 bearing
    bearing1 = bearing(p1, p2)
    # p2 to p3 bearing
    bearing2 = bearing(p2, p3)
    # angle between the bearings
    angle = bearing2 - bearing1
    if angle < 0:
        angle += 360
    return angle

def find_cornerpoints(geom):
    boundspoly = Polygon([(geom.bounds[0], geom.bounds[1]), (geom.bounds[2], geom.bounds[1]), (geom.bounds[2], geom.bounds[3]), (geom.bounds[0], geom.bounds[3])])
    centroid = boundspoly.centroid


    df_arry = []

    if geom.geom_type == 'MultiPolygon':
        for polygon in geom.geoms:
            coords_df = pd.DataFrame(polygon.exterior.coords, columns=['x', 'y'])
            coords_df['bearing'] = coords_df.apply(lambda row: bearing(centroid, row), axis=1)
            coords_df['distance'] = coords_df.apply(lambda row: distance(centroid, row), axis=1)
            df_arry.append(coords_df)

    
    elif geom.geom_type == 'Polygon':
        coords_df = pd.DataFrame(geom.exterior.coords, columns=['x', 'y'])
        coords_df['bearing'] = coords_df.apply(lambda row: bearing(centroid, row), axis=1)
        coords_df['distance'] = coords_df.apply(lambda row: distance(centroid, row), axis=1)
        df_arry.append(coords_df)
            

    # merge the dataframes
    points = pd.concat(df_arry)


    try:
        nw_point = points[points['bearing'] >= 270].sort_values('distance', ascending=False).iloc[0]
    except IndexError:
        nw_point = None

    try:
        ne_point = points[(points['bearing'] > 0) & (points['bearing'] <= 90)].sort_values('distance', ascending=False).iloc[0]
    except IndexError:
        ne_point = None

    try:
        se_point = points[(points['bearing'] > 90) & (points['bearing'] <= 180)].sort_values('distance', ascending=False).iloc[0]
    except IndexError:
        se_point = None

    try:
        sw_point = points[(points['bearing'] > 180) & (points['bearing'] <= 270)].sort_values('distance', ascending=False).iloc[0]
    except IndexError:
        sw_point = None
        
    # if any([nw_point is None, ne_point is None, se_point is None, sw_point is None]):
    #     return nw_point, ne_point, se_point, sw_point
    if nw_point is not None:
        nw_point = Point(nw_point['x'], nw_point['y'])
    if ne_point is not None:
        ne_point = Point(ne_point['x'], ne_point['y'])
    if se_point is not None:
        se_point = Point(se_point['x'], se_point['y'])
    if sw_point is not None:
        sw_point = Point(sw_point['x'], sw_point['y'])
    
    return nw_point, ne_point, se_point, sw_point


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


def subdivide_polygon(polygon):
    # according to this https://multco-web7-psh-files-usw2.s3-us-west-2.amazonaws.com/s3fs-public/Exhibit%20I.5.1-%20BLM%20Survey%20Manual%20-%20Excerpts%20on%20Water%20Boundaries.pdf
    # BLM  Manual of Surveying Instructions 2009
    # lets find the midpoints of the polygon sides and findt he closest point to the midpoint on the polygon vertices
    # then we will create a new polygon with the midpoints and the closest points

    # find the corner points of the polygon
    nw_point, ne_point, se_point, sw_point = find_cornerpoints(polygon)

    # get north side midpoint using corner points
    north_side_midpoint = Point((ne_point.x + nw_point.x) / 2, (ne_point.y + nw_point.y) / 2)
    # get east side midpoint using corner points
    east_side_midpoint = Point((ne_point.x + se_point.x) / 2, (ne_point.y + se_point.y) / 2)
    # get south side midpoint using corner points
    south_side_midpoint = Point((se_point.x + sw_point.x) / 2, (se_point.y + sw_point.y) / 2)
    # get west side midpoint using corner points
    west_side_midpoint = Point((nw_point.x + sw_point.x) / 2, (nw_point.y + sw_point.y) / 2)

    # find center point from drapwin from the midpoints
    e_w_line = LineString([west_side_midpoint, east_side_midpoint])
    n_s_line = LineString([north_side_midpoint, south_side_midpoint])
    center_point = e_w_line.intersection(n_s_line)

    # create 4 new polygons from the midpoints and center point
    polygon_nw = Polygon([nw_point, north_side_midpoint, center_point, west_side_midpoint])
    polygon_ne = Polygon([north_side_midpoint, ne_point, east_side_midpoint, center_point])
    polygon_se = Polygon([center_point, east_side_midpoint, se_point, south_side_midpoint])
    polygon_sw = Polygon([west_side_midpoint, center_point, south_side_midpoint, sw_point])





    return [polygon_nw, polygon_ne, polygon_se, polygon_sw]




def irregular_subdivision(polygon, side_length):
    # according to this https://multco-web7-psh-files-usw2.s3-us-west-2.amazonaws.com/s3fs-public/Exhibit%20I.5.1-%20BLM%20Survey%20Manual%20-%20Excerpts%20on%20Water%20Boundaries.pdf
    # find the corner to layout the 2nd division
    # 1. Find the corner points of the polygon
    nw_point, ne_point, se_point, sw_point = find_cornerpoints(polygon)
   


    north_distance = distance(nw_point, ne_point)
    east_distance = distance(ne_point, se_point)
    south_distance = distance(se_point, sw_point)
    west_distance = distance(sw_point, nw_point)
    
    x_sides_avg = (north_distance + south_distance) / 2
    y_sides_avg = (east_distance + west_distance) / 2

    # get avg and std deviation of the distances
    x_std_dev = math.sqrt(((north_distance - x_sides_avg) ** 2 + (south_distance - x_sides_avg) ** 2) / 2)
    y_std_dev = math.sqrt(((east_distance - y_sides_avg) ** 2 + (west_distance - y_sides_avg) ** 2) / 2)


    # if the std deviation is less than 40 then we can assume that the polygon is a square or close to a square
    # find what is longer x or y average
    if x_sides_avg > y_sides_avg:
        side_length = x_sides_avg/2
    else:
        side_length = y_sides_avg /2





    # get the closest to a right angle corner
    angles = []

    if sw_point is not None and se_point is not None and nw_point is not None:
        sw_angle = angle(se_point, sw_point, nw_point)
        angles.append({"point" : 'SW', "angle": sw_angle, "diff": abs(90 - sw_angle)})
    else:
        sw_angle = 1000

    if se_point is not None and sw_point is not None and ne_point is not None:
        se_angle = angle(ne_point, se_point, sw_point)
        angles.append({"point" : 'SE', "angle": se_angle, "diff": abs(90 - se_angle)})
    else:
        se_angle = 1000

    if nw_point is not None and ne_point is not None and sw_point is not None:
        nw_angle = angle(sw_point, nw_point, ne_point)
        angles.append({"point" : 'NW', "angle": nw_angle, "diff": abs(90 - nw_angle)})
    else:
        nw_angle = 1000

    if ne_point is not None and nw_point is not None and se_point is not None:
        ne_angle = angle(nw_point, ne_point, se_point)
        angles.append({"point" : 'NE', "angle": ne_angle, "diff": abs(90 - ne_angle)})
        
    else:
        ne_angle = 1000


    

    # order the angles by the difference
    angles = sorted(angles, key=lambda x: x['diff'], reverse=False)

    
    # find the closest corener to 90 degrees
    corner = angles[0]['point']




    if x_std_dev < 40 and y_std_dev < 40:
        corner ="SW"

    
   



    polygons = []
    if corner == 'NE':
        rootangle = bearing(ne_point, nw_point)

        #project all the points
        point1 = ne_point
        point2 = project_point(point1, rootangle, side_length)
        point3 = project_point(point2, rootangle, side_length)
        point4 = project_point(point1, rootangle - 90, side_length)
        point5 = project_point(point4, rootangle, side_length)
        point6 = project_point(point5, rootangle, side_length)
        point7 = project_point(point4, rootangle - 90, side_length)
        point8 = project_point(point7, rootangle, side_length)
        point9 = project_point(point8, rootangle, side_length)

        # create the polygons
        #north east polygon
        polygons.append(Polygon([point1, point2, point5, point4]))
        # #north west polygon
        polygons.append(Polygon([point2, point3, point6, point5]))
        # #south west polygon
        polygons.append(Polygon([point6, point5, point8, point9]))
        # #south east polygon
        polygons.append(Polygon([point5, point4, point7, point8]))

    elif corner == 'NW':
        rootangle = bearing(nw_point, ne_point)

        #project all the points
        point1 = nw_point
        point2 = project_point(point1, rootangle, side_length)
        point3 = project_point(point2, rootangle, side_length)
        point4 = project_point(point1, rootangle + 90, side_length)
        point5 = project_point(point4, rootangle, side_length)
        point6 = project_point(point5, rootangle, side_length)
        point7 = project_point(point4, rootangle + 90, side_length)
        point8 = project_point(point7, rootangle, side_length)
        point9 = project_point(point8, rootangle, side_length)

        # create the polygons
        #north west polygon
        polygons.append(Polygon([point1, point2, point5, point4]))
        # #north east polygon
        polygons.append(Polygon([point2, point3, point6, point5]))
        # #south east polygon
        polygons.append(Polygon([point6, point5, point8, point9]))
        # #south west polygon
        polygons.append(Polygon([point5, point4, point7, point8]))


    elif corner == 'SW':
        rootangle = bearing(sw_point, se_point)

        #project all the points
        point1 = sw_point
        point2 = project_point(point1, rootangle, side_length)
        point3 = project_point(point2, rootangle, side_length)
        point4 = project_point(point1, rootangle - 90, side_length)
        point5 = project_point(point4, rootangle, side_length)
        point6 = project_point(point5, rootangle, side_length)
        point7 = project_point(point4, rootangle - 90, side_length)
        point8 = project_point(point7, rootangle, side_length)
        point9 = project_point(point8, rootangle, side_length)

        # create the polygons
        #south west polygon
        polygons.append(Polygon([point1, point2, point5, point4]))
        # #north west polygon
        polygons.append(Polygon([point2, point3, point6, point5]))
        # #north east polygon
        polygons.append(Polygon([point6, point5, point8, point9]))
        # #south east polygon
        polygons.append(Polygon([point5, point4, point7, point8]))



    elif corner == 'SE':
        rootangle = bearing(se_point, sw_point)

        #project all the points
        point1 = se_point
        point2 = project_point(point1, rootangle, side_length)
        point3 = project_point(point2, rootangle, side_length)
        point4 = project_point(point1, rootangle + 90, side_length)
        point5 = project_point(point4, rootangle, side_length)
        point6 = project_point(point5, rootangle, side_length)
        point7 = project_point(point4, rootangle + 90, side_length)
        point8 = project_point(point7, rootangle, side_length)
        point9 = project_point(point8, rootangle, side_length)

        # create the polygons
        #south east polygon
        polygons.append(Polygon([point1, point2, point5, point4]))
        # #south west polygon
        polygons.append(Polygon([point2, point3, point6, point5]))
        # #north west polygon
        polygons.append(Polygon([point6, point5, point8, point9]))
        # #north east polygon
        polygons.append(Polygon([point5, point4, point7, point8]))


        # trim the polygons to the original polygon
    polygons = [polygon.intersection(poly) for poly in polygons]
    # remove empty polygons
    polygons = [poly for poly in polygons if poly.is_empty == False]




   

    return polygons
