"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("cta.station.arrivals.v1", value_type=Station)
out_topic = app.topic("cta.station.arrivals.transformed.v1", partitions=1)
table = app.Table(
   "cta.stations.table.v1",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def process(stations_stream):
    async for station in stations_stream:
        
        line = None
        if station.red == True:
            line = 'red'
        elif station.green == True:
            line = 'green'
        elif station.blue == True:
            line= 'blue'
        else:
            logger.info(f"{station.station_id} has no line color.")
            
            
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )
        
        table[stream.station_id] = transformed_station
        

if __name__ == "__main__":
    app.main()
