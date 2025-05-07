import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from gcloud_data import DATASET_NAME, TABLE_NAME_CYCLE_HIRE, TABLE_NAME_CYCLE_STATION, PROJECT_ID, BUCKET_PATH
from apache_beam.pvalue import PCollection
from apache_beam import Pipeline
#These datasets were preloaded using BigQuery
#NOTE: How would I need to do this if I wanted to load them straight from public?
BIGQUERY_TABLE_CYCLE_HIRE = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME_CYCLE_HIRE}"
BIGQUERY_TABLE_CYCLE_STATION = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME_CYCLE_STATION}"

#This is obviously generated from genai but seemed to make sense and comply with the formula on wikipedia.
def haversine_distance(elem):
    """
    Calculates the distance in kilometers between two points on Earth
    given their latitude and longitude coordinates.

    Args:
        lat1 (float): Latitude of the first point in degrees.
        lon1 (float): Longitude of the first point in degrees.
        lat2 (float): Latitude of the second point in degrees.
        lon2 (float): Longitude of the second point in degrees.

    Returns:
        float: The distance between the two points in kilometers.
    """
    import math
    lat1=elem.get("start_station_latitude") 
    lon1=elem.get("start_station_longitude") 
    lat2=elem.get("end_station_latitude") 
    lon2=elem.get("end_station_longitude")
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        elem["distance"] = 0
        yield elem
        return

    # Radius of the Earth in kilometers, it is 
    R = 6371.0 #Debatable if its 6378 instead.

    # Convert degrees to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # Differences in coordinates
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    elem['distance'] = distance * elem["travel_count"]
    yield elem

def format_travel_count(element) -> str:
    """Formats the travel count data into the desired string format."""
    return f"{element['start_station_id']},{element['end_station_id']},{element['travel_count']}"

def get_hires_raw(p:Pipeline) -> PCollection:
    """Returns the hires raw data"""
    return p | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(
            query=f"""
                    SELECT 
                        start_station_id, 
                        end_station_id, 
                        start_station_name,
                        end_station_name
                    FROM `{BIGQUERY_TABLE_CYCLE_HIRE}` 
                    """,
            use_standard_sql=True
        )

def get_stations_raw(p:Pipeline) -> PCollection:
    """Returns the stations raw data"""
    return p | "ReadStationsFromBigQuery" >> beam.io.ReadFromBigQuery(
            query=f"""
                    SELECT
                        id,
                        name,
                        latitude,
                        longitude
                    FROM `{BIGQUERY_TABLE_CYCLE_STATION}`
                    """,          
            use_standard_sql=True
        )

def get_clean_hires(hires:PCollection, stations:PCollection) -> PCollection:
    """Returns cleaned travel counts data using the stations data (NOTE: check EDA for more info)"""
    #dict to retrieve id's for station names
    #technique called 'side inputs', basically broadcast join.
    stations_name_to_id:PCollection = stations | "MapStationNameToId" >> beam.Map(
        lambda row: (row["name"], row["id"])
    )
    # fixed_hires = hires | "FixStationId" >> beam.Map(
    #     lambda hire, station_name_to_id: {
    #         "start_station_id": hire["start_station_id"] if hire["start_station_id"] else station_name_to_id.get(hire["start_station_name"]),
    #         "end_station_id": hire["end_station_id"] if hire["end_station_id"] else station_name_to_id.get(hire["end_station_name"]),
    #     },
    #     station_name_to_id=beam.pvalue.AsDict(stations_name_to_id)
    #)
    cleaned_hires = hires | "FilterOutNullStationIds" >> beam.Filter(
            lambda row: row['start_station_id'] is not None and row['end_station_id'] is not None #and row['start_station_id'] != row['end_station_id']
        )
    return cleaned_hires

def get_hire_counts(hires:PCollection) -> PCollection:
    """Create key-value pairs of ((start_station_id, end_station_id), 1)"""
    station_pairs = hires | "CreateStationPairs" >> beam.Map(
            lambda hire: ((hire['start_station_id'], hire['end_station_id']), 1)
        )
    return station_pairs | "GroupAndCount" >> beam.CombinePerKey(sum)

def get_counts_and_total_distances(count_hires:PCollection, stations:PCollection) -> PCollection|None:
    filtered_stations :PCollection = stations | "RemoveNullCoordinates" >> beam.Filter(
        lambda row: row["latitude"] is not None and row["longitude"] is not None)
    stations_id_to_lat:PCollection = filtered_stations | "MapStationIdToLat" >> beam.Map(
        lambda row: (row["id"], row["latitude"])
    )
    stations_id_to_lon:PCollection = filtered_stations | "MapStationIdToLon" >> beam.Map(
        lambda row: (row["id"], row["longitude"])
    )
    station_pairs = count_hires | "CreateTravelInformation" >> beam.Map(
            lambda hire, stations_id_to_lat, stations_id_to_lon: {
                    "start_station_id": hire[0][0],
                    "end_station_id": hire[0][1],
                    "travel_count": hire[1],
                    "start_station_latitude": stations_id_to_lat.get(hire[0][0]),
                    "start_station_longitude": stations_id_to_lon.get(hire[0][0]),
                    "end_station_latitude": stations_id_to_lat.get(hire[0][1]),
                    "end_station_longitude": stations_id_to_lon.get(hire[0][1]),
            },
                        stations_id_to_lat=beam.pvalue.AsDict(stations_id_to_lat),
                        stations_id_to_lon=beam.pvalue.AsDict(stations_id_to_lon)
    
    ) | "CalculateDistances" >> beam.ParDo(haversine_distance) | "FilterInvalidDistances" >> beam.Filter(
        lambda row: row["distance"] != 0 and row["start_station_id"] != row["end_station_id"]
    )
    return station_pairs

def write_to_txt(data:PCollection, location:str, filename:str) -> None:
    _ = data | "WriteToGCS" >> beam.io.WriteToText(
            location,
            file_name_suffix=f"{filename}.txt",
            num_shards=1,
            shard_name_template=''
        )

def run_easy(pipeline_options:PipelineOptions):
    with beam.Pipeline(options=pipeline_options) as pipeline:
        #p is the pipeline, every step is formatted as "p add <'step_name'> do <operation>""
        #NOTE: see exploratory data analysis to find out that there are NULLs that can be resolved using the 'stations' dataset.
        hires:PCollection = get_hires_raw(pipeline)
        stations:PCollection = get_stations_raw(pipeline)
        clean_hires:PCollection = get_clean_hires(hires, stations)
        count_hires:PCollection = get_hire_counts(clean_hires)
        #renamed the none values that are left to '0' so we can have those datapoints available, even when there's no corresponding id.
        #if this doesn't work, we will probably have to artifically add id's for these locations
        #i presume though, that this wont be necessary and we can either filter these points out or replace them by 0's, since the validator has no way of knowing these artificial id's.
        formatted_output = count_hires | "FormatOutputString" >> beam.Map(
            lambda item: f"{item[0][0]},{item[0][1]},{item[1]}"
        )
        #Sorting is impossible in apache beam. Beam is considered a bad choice for datasets that require ordering.
        #Though this is unnecessary since the validation will do this for us.
        write_to_txt(formatted_output, f"{BUCKET_PATH}output/", "output_easy")

def run_hard(pipeline_options:PipelineOptions):
    with beam.Pipeline(options=pipeline_options) as pipeline:
        #p is the pipeline, every step is formatted as "p add <'step_name'> do <operation>""
        #NOTE: see exploratory data analysis to find out that there are NULLs that can be resolved using the 'stations' dataset.
        hires:PCollection = get_hires_raw(pipeline)
        stations:PCollection = get_stations_raw(pipeline)
        clean_hires:PCollection = get_clean_hires(hires, stations)
        count_hires:PCollection = get_hire_counts(clean_hires)
        calculate_distances:PCollection = get_counts_and_total_distances(count_hires, stations)
        formatted_output = calculate_distances | "FormatOutputString" >> beam.Map(
            lambda item: f"{item["start_station_id"]},{item["end_station_id"]},{item["travel_count"]},{item["distance"]}"
        )
        #Sorting is impossible in apache beam. Beam is considered a bad choice for datasets that require ordering.
        #Though this is unnecessary since the validation will do this for us.
        write_to_txt(formatted_output, f"{BUCKET_PATH}output/", "output_hard")

if __name__ == "__main__":
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region="europe-west1", 
        temp_location="gs://test-bicycles-src/tmp"
    )
    #run_easy(pipeline_options)
    run_hard(pipeline_options)