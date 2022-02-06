import argparse
import folium
import pandas as pd
import re
from geopy.geocoders import Nominatim
import time
start = time.time()

##map = folium.Map()
##map.save('Map_1.html')
#def read_MIDI(file: str)->list:
#    """
#    reads MIDI and returns coordinats of the note, its name,
#    duration
#    :param str file:
#
#    :return List[tuple]
#    
#    """
#    
#    with open(file, 'rb') as file_contents:
#        linehex = file_contents.read().hex()
#        line_sep_hex = re.findall('..', linehex)
#    with open("locations2.list", 'w', encoding="utf-8", errors="ignore") as file_contents:
#        for line in line_sep_hex:
#            if line == "0a":
#                file_contents.write(line+"\n")
#            else:
#                file_contents.write(line+" ")
#    
def format_line(position: int, line_sep: str)->str:
    """
    formats line to last 3 names
    and gets rid of appendixes in brackets
    :param int position:
    :param str line_sep:
    :return str for_search_loc
    
    """
    loc_list = line_sep[-position].split(',')
    len_loc_list = len(loc_list)
    for_search_loc_list = []
    if len_loc_list > 3:
        for_search_loc_list = loc_list[-3:]
        for_search_loc = ",".join(for_search_loc_list)
        for_search_loc.replace("\n","")
    else:
        for_search_loc = ",".join(loc_list)
        for_search_loc.replace("\n","")
    return for_search_loc
def search_by_year(file: str, year: str)->list:
    """
    searches in locations.list films that are puplished in the
    asked year
    :param str file:
    :param str year:

    :return list lines
    
    """
    geolocator = Nominatim(user_agent="serhii.ivanov@ucu.edu.ua")
    with open(file, 'r') as file_contents:
        lines = []
        #count = 0
        used_locations = {}
        for line in file_contents:
            if f"({year})" in line:
                #count+=1
                line_sep = line.split("\t")
                if "(" in line_sep[-1]:
                    for_search_loc = format_line(2, line_sep)
                else:
                    for_search_loc = format_line(1, line_sep)
                #print(for_search_loc)
                
                if for_search_loc not in used_locations:
                    location = geolocator.geocode(for_search_loc)
                    if location != None:
                        location_latitude = location.latitude
                        location_longitude = location.longitude
                        used_locations[for_search_loc] = [location_latitude, location_longitude]
                        lines.append([line_sep[0], for_search_loc, location_latitude, location_longitude])
                else:
                    location_latitude = used_locations[for_search_loc][0]
                    location_longitude = used_locations[for_search_loc][1]
                    lines.append([line_sep[0], for_search_loc, location_latitude, location_longitude])
                #if location != None:
#
                #    used_locations[line_sep[-1]] = [location_latitude, location_longitude]
                #    lines.append([line_sep[0],line_sep[-1].replace("\n",""), location_latitude, location_longitude])
                
                
                    #print(location.address)
                    #print((location.latitude, location.longitude))
               
                #print("--------------------------------------------")
                #print([line_sep[0],line_sep[-1]])
                
            #if count == 50:
            #    #print(lines)
            #    break
    print(f"done reading in {abs(start - time.time())}")
    return lines


def write_tsv(lines: list):
    with open("locations.tsv", 'w') as file_contents:
        file_contents.write("Film\tLocation\tLat\tLon\n")
        for line in lines:
            file_contents.write((f'{line[0]}\t{line[1]}\t{line[2]}\t{line[3]}\n'))
                
    print(f"done writing in {abs(start - time.time())}")
    #data = pd.read_csv("locations.tsv", sep='\t', encoding = "ISO-8859-1")


def create_html_map(year):
    map = folium.Map(location=[48.314775, 25.082925],
    zoom_start=10)
    html = """<h4>Film information:</h4>
    Year: {},<br>
    Film name: {}
    """
    fg_list = []
    data = pd.read_csv("locations.tsv", sep='\t', encoding = "ISO-8859-1")
    lat = data['Lat']
    lon = data['Lon']
    films = data['Film']
    fg = folium.FeatureGroup(name=year)
    for lt, ln, fl in zip(lat, lon, films):
        iframe = folium.IFrame(html=html.format(year, fl),
                               width=300,
                               height=100)
        fg.add_child(folium.Marker(location=[lt, ln],
                     popup=folium.Popup(iframe),
                     icon=folium.Icon(color = "red")))
    fg_list.append(fg)
    for fg in fg_list:
        map.add_child(fg)
    map.add_child(folium.LayerControl())
    map.save('Map_5.html')

    #pd.read_csv("locations.tsv", sep='\t', encoding = "ISO-8859-1")
    return
"""
if __name__ == '__main__':
    import doctest
    doctest.testmod()
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('integers', metavar='N', type=int, nargs='+',
                        help='an integer for the accumulator')
    parser.add_argument('--sum', dest='accumulate', action='store_const',
                        const=sum, default=max,
                        help='sum the integers (default: find the max)')

    args = parser.parse_args()
    print(args.accumulate(args.integers))
"""
def main(file, year):
    lines = search_by_year(file, year)
    write_tsv(lines)
    create_html_map(year)
    print(f"done in {abs(start - time.time())}")
main("locationsmini.list", "2000")
#read_MIDI("locationsmini.list")
