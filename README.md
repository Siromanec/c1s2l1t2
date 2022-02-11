# c1s2l1t2
course1:semester2:lab1:task2
## Performance
The program uses multiprocessing to send multiple requests
``` python
def process_addresses(locations, def_lat, def_lon) -> list:
    locations_with_def = {(x, def_lat, def_lon) for x in locations}
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    result = pool.map(geocode_worker_dumm, locations_with_def)
```
But this brings its risks. If you send to many requests you might get blocked from using geocoding service, as it was for me with OpenMapQuest.
Thus I use Photon.

With OpenMapQuest it found all locations in list of first 10000 films in 2000 in 16 seconds. Photon does it in 40.
## Requirements
1. numpy
2. follium
3. pandas
4. geocode
