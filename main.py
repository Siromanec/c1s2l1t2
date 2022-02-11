
import functools
import folium
import pandas as pd
from geopy.geocoders import OpenMapQuest
from geopy.geocoders import GoogleV3
import multiprocessing
import numpy as np
from geopy.geocoders import Photon
from geopy.extra.rate_limiter import RateLimiter
import argparse
import time


#geolocator = OpenMapQuest(api_key="MFgzET4u03HgHWAEqrJPk1XF8MNrWIJl")
#geolocator = GoogleV3()
geolocator = Photon(user_agent="measurements")
#geocode = RateLimiter(geolocator, min_delay_seconds=0.001)
def memoize(function):
    memo = {}
    @functools.wraps(function)
    def wrapper(*args):
        signature = (function, args)
        if signature in memo:
            return memo[signature]
        else:
            rv = function(*args)
            memo[signature] = rv
            return rv
    return wrapper


def format_line(position: int, line_sep: str)->str:
    """
    formats line to last 3 names
    and gets rid of appendixes in brackets
    :param int position:
    :param str line_sep:
    :return str for_search_loc
    
    """
    if "{" in line_sep[0]:
        film_split = line_sep[0].split('{')
        film = film_split[0]
    else:
        film = line_sep[0]
    loc_list = line_sep[-position].split(',')
    len_loc_list = len(loc_list)
    for_search_loc_list = []
    if len_loc_list > 3:
        for_search_loc_list = loc_list[-3:]
        for_search_loc = ",".join(for_search_loc_list)
    else:
        for_search_loc = ",".join(loc_list)
    return for_search_loc, film


@memoize
def geocode_worker_2(address: str, count = 0) -> tuple:
    """
    :param str address: address to locate
    :param int count: is to count how many times the address was cut
    :return float lat: latitude of the address
    :return float lon: longitude of the address
    >>> geocode_worker_2("Oxfordshire, South East England, England, United Kingdom")
    (51.833333, -1.25)
    >>> geocode_worker_2("Boston, Suffolk County, Massachusetts, 02102, United States")
    (42.3617016, -71.0568342)
    >>> geocode_worker_2("Merseyside, North West England, England, United Kingdom,")
    (53.4013379, -2.9927496)
    """
    coords = geolocator.geocode(address, timeout= None)
    if coords == None:
        split_at = address.index(",")
        drop_first = address[split_at + 2:]
        return geocode_worker_2(drop_first, count + 1)
    elif count >= 3:
        return None, None
    else:
        lat = coords.latitude
        lon = coords.longitude
        return lat, lon



@memoize
def geocode_worker(address, def_lat, def_lon):
    """
    function to summon calculation of latitude and longitude,
    relative haversine distance
    and put all values in order
    >>> geocode_worker("Oxfordshire, South East England, England, United Kingdom", 49.83826, 24.0232)
    ('Oxfordshire, South East England, England, United Kingdom', (51.833333, -1.25, 0.27930985741983094))
    >>> geocode_worker("Boston, Suffolk County, Massachusetts, 02102, United States", 49.83826, 24.0232)
    ('Boston, Suffolk County, Massachusetts, 02102, United States', (42.3617016, -71.0568342, 1.0783930669555306))
    >>> geocode_worker("Merseyside, North West England, England, United Kingdom,", 49.83826, 24.0232)
    ('Merseyside, North West England, England, United Kingdom,', (53.4013379, -2.9927496, 0.29737811851153095))
    """

    lat, lon = geocode_worker_2(address)
    return address, (lat, lon, distance_haversine(lat, def_lat, lon, def_lon)) 


def geocode_worker_dumm(locations_with_def):
    """
    dummy function just to send values from
    location_with_def to geocode_worker()
    """
    address, def_lat, def_lon = locations_with_def
    return geocode_worker(address, def_lat, def_lon)


def process_addresses(locations, def_lat, def_lon) -> list:
    """
    return the result of parsing all locations
    (address: str, (latitude: float, longitude: float, haversine: float))
    {(address1, (latitude1, longitude1, haversine1)),
    (address2, (latitude2, longitude2, haversine2)),
    (address3, (latitude3, longitude3, haversine3))
    >>> data = process_addresses({"Oxfordshire, South East England, England, United Kingdom",
    ...                           "Boston, Suffolk County, Massachusetts, 02102, United States",
    ...                           "Merseyside, North West England, England, United Kingdom,"},
    ...                           49.83826, 24.0232)
    >>> len(data) == 3
    True
    """
    # Start as many worker processes as you have cores
    locations_with_def = {(x, def_lat, def_lon) for x in locations}
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    # Apply geocode worker to each address, asynchronously
    result = pool.map(geocode_worker_dumm, locations_with_def)
    return result


def locate_coords(locations: set, def_lat: float, def_lon: float)->tuple:
    """
    turn the result of process_addresses into dictionary
    to access them when needed for other functions
    :param set locations:
    :param float def_lat:
    :param float def_lon:
    :return dict coords:
    >>> coords = locate_coords({"Oxfordshire, South East England, England, United Kingdom",
    ...                         "Boston, Suffolk County, Massachusetts, 02102, United States",
    ...                         "Merseyside, North West England, England, United Kingdom,"},
    ...                         49.83826, 24.0232)
    >>> coords['Oxfordshire, South East England, England, United Kingdom']
    (51.833333, -1.25, 0.27930985741983094)
    """
    coords = dict(tuple(process_addresses(locations, def_lat, def_lon)))

    return coords


def distance_haversine(lat: float, def_lat: float, lon: float, def_lon: float) -> float:
    """
    calculates relative distance using haversine equasion
    :param float lat: latitude of the address
    :param float def_lat: defaul longitude
    :param float lon: longitude of the address
    :param float def_lon: default longitude
    :return float relative_distance: relative haversine distance
    >>> distance_haversine(49, 49.83826, 24, 24.0232)
    0.014632767772867672
    >>> distance_haversine(50, 49.83826, 25, 24.0232)
    0.011333980095445497
    >>> distance_haversine(63, 49.83826, 34, 24.0232)
    0.24841457803560293
    """
    relative_distance = 2 * np.arcsin(np.sqrt((np.sin(((def_lat-lat)/2) * (np.pi/180)))**2 + 
                                      np.cos(lat * (np.pi/180)) *
                                      np.cos(def_lat * (np.pi/180)) *
                                     (np.sin(((def_lon-lon)/2) * (np.pi/180)))**2))
    return relative_distance


def search_by_year(file: str, year: str) -> tuple:
    """
    searches in given file films that are puplished in the
    asked year
    :param str file: location to the file
    :param str year: year to search for
    :return set lines: set of all locations
    :return dict films: dict with all locations for the film
    """
    with open(file, 'r') as file_contents:
        locations = set()
        films = {}
        for line in file_contents:
            if f"({year})" in line:
                line_sep = line.split("\t")
                if "(" in line_sep[-1]:
                    for_search_loc, film = format_line(2, line_sep)
                else:
                    for_search_loc, film = format_line(1, line_sep)
                    for_search_loc = for_search_loc[:-1]
                locations.add(for_search_loc)
                if film not in films:
                    films[film] = set()
                    films[film].add(for_search_loc)
                else:
                    films[film].add(for_search_loc)
    return locations, films


def parse_locations(locations: set, films: dict, def_lat: float, def_lon: float) -> set:
    """
    This function parses locations into set of markers with their location and
    relative haversine distance
    :param set locations: all locations
    :param dict films: 
    :param float def_lat: default latitude
    :param float def_lon: default longitude
    :return set markers: set of markers which will be later written into tsv file
    """
    locations_with_coords = locate_coords(locations, def_lat, def_lon)
    markers = set()
    for film in films:
        for location in films[film]:
            lat, lon, haversine = locations_with_coords[location]
            if lat != None:
                markers.add((film, location, lat, lon, haversine))
    return markers


def write_tsv(markers: set) -> None:
    """
    writes tsv file with given markers:
    "Film\tLocation\tLat\tLon\tHaversine\n"
    """
    with open("locations.tsv", 'w') as file_contents:
        file_contents.write("Film\tLocation\tLat\tLon\tHaversine\n")
        for marker in markers:
            file_contents.write((f'{marker[0]}\t{marker[1]}\t{marker[2]}\t{marker[3]}\t{marker[4]}\n'))          


def create_html_map(year: str, def_lat: float, def_lon: float) -> None:
    """
    creates an html map
    :param str year: year to display
    :param float def_lat: default latitude
    :param float def_lon: default longitude

    :return None
    """
    map = folium.Map(location=[def_lat, def_lon],
    zoom_start=10)
    html = """<h4>Film information:</h4>
    Year: {},<br>
    Film name: {}
    """
    fg_list = []
    data = pd.read_csv("locations.tsv", sep='\t', encoding = "ISO-8859-1")
    data_sorted = data.sort_values("Haversine")
    data_sorted_and_cut = data_sorted.head(11)
    lat = data_sorted_and_cut['Lat']
    lon = data_sorted_and_cut['Lon']
    films = data_sorted_and_cut['Film']
    fg = folium.FeatureGroup(name=year)
    for lt, ln, flm in zip(lat, lon, films): 
        iframe = folium.IFrame(html=html.format(year, flm),
                               width=3000,
                               height=1000)
        fg.add_child(folium.Marker(location=[lt, ln],
                     popup=folium.Popup(iframe),
                     icon=folium.Icon(color = "red")))
    fg_list.append(fg)
    for fg in fg_list:
        map.add_child(fg)
    map.add_child(folium.LayerControl())
    map.save('Map.html')


def main(file: str, year: str, def_lat: float, def_lon: float) -> None:
    """
    the main function
    :param str file:
    :param str year:
    :param float def_lat:
    :param float def_lon:

    :return None
    """
    start = time.time()
    locations, films = search_by_year(file, year)
    markers = parse_locations(locations, films, def_lat, def_lon)
    write_tsv(markers)
    create_html_map(year, def_lat, def_lon)
    print(f"done in {abs(start - time.time())}")
if __name__ == '__main__':
    import doctest
    doctest.testmod()
    #multiprocessing.freeze_support()
    #main("locationsmini.list", "2014", 49.83826, 24.0232)


if __name__ == '__main__':
    #x = map(geocode_worker, addresses)
    #x = map(geocode_worker, addresses)
    #x = map(geocode_worker, addresses)
    #x = map(geocode_worker, addresses)
    #print("-------------")
    #print(list(x))
    #print("-------------")
    #print(main())
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('year', type=str,
                        help='the year you wnt to search for')
    parser.add_argument('latitude', type= float,
                        help='latitude of center location')
    parser.add_argument('longitude', type= float,
                        help='longitude of center location')
    parser.add_argument('file', type= str,
                        help='file location')
    


    args = parser.parse_args()
    args_dict = vars(args)
    year = args_dict["year"]
    lat = args_dict["latitude"]
    lon = args_dict["longitude"]
    file = args_dict["file"]
    multiprocessing.freeze_support()
    main(file, year, lat, lon)

    #print(args.accumulate(args.integers))
