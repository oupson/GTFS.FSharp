module GtfsReader

open System.IO
open System.IO.Compression
open FSharp.Data
open Npgsql
open Npgsql.FSharp
open NetTopologySuite.Geometries

type RouteType =
    | Tram = 0
    | Subway = 1
    | Rail = 2
    | Bus = 3
    | Ferry = 4
    | CableTram = 5
    | AerialLift = 6
    | Funicular = 7
    | TrolleyBus = 11
    | Monorail = 12

type TripDirection =
    | OneDirection = 0
    | TheOpposite = 1

let entryLineStream (entry: ZipArchiveEntry) =
    new StreamReader(entry.Open())
    |> Seq.unfold (fun sr ->
        match sr.ReadLine() with
        | null ->
            sr.Dispose()
            None
        | str -> Some(str, sr))

let truncateTable table database =
    database
    |> Sql.existingConnection
    |> Sql.query $"TRUNCATE %s{table} RESTART IDENTITY CASCADE"
    |> Sql.executeNonQuery
    |> ignore

    database

let createTemporaryTables database =
    database
    |> Sql.existingConnection
    |> Sql.query "CREATE TEMPORARY TABLE GTFS_CALENDAR_DATES(service_id TEXT, date TEXT, exception_type INTEGER)"
    |> Sql.executeNonQuery
    |> ignore

    database
    |> Sql.existingConnection
    |> Sql.query
        "CREATE TEMPORARY TABLE GTFS_TRIP(route_id TEXT,service_id TEXT,trip_id TEXT,direction_id INTEGER,shape_id TEXT)"
    |> Sql.executeNonQuery
    |> ignore

    database
    |> Sql.existingConnection
    |> Sql.query
        "CREATE TEMPORARY TABLE GTFS_STOP_TIMES(trip_id TEXT,arrival_time TEXT,departure_time TEXT,stop_id TEXT,stop_sequence INTEGER,pickup_type INTEGER)"
    |> Sql.executeNonQuery
    |> ignore

    database

let readAgencies (archive: ZipArchive) (connection: NpgsqlConnection) =
    use agencies = archive.GetEntry("agency.txt").Open() |> CsvFile.Load

    for row in agencies.Rows do
        connection
        |> Sql.existingConnection
        |> Sql.query "INSERT INTO AGENCY(agencyId, agencyName, agencyUrl) VALUES (@id,@name,@url)"
        |> Sql.parameters
            [ "@id", row.GetColumn("agency_id") |> Sql.text
              "@name", row.GetColumn("agency_name") |> Sql.text
              "@url", row.GetColumn("agency_url") |> Sql.text ]
        |> Sql.executeNonQuery
        |> ignore

    connection

let readRoutes (archive: ZipArchive) (connection: NpgsqlConnection) =
    use routes = archive.GetEntry("routes.txt").Open() |> CsvFile.Load

    for route in routes.Rows do
        connection
        |> Sql.existingConnection
        |> Sql.query
            "INSERT INTO ROUTE(routeId, routeShortName, routeLongName, routeType, agencyId) VALUES (@id, @sn, @ln, @ty, @aid)"
        |> Sql.parameters
            [ "@id", route.GetColumn("route_id") |> Sql.text
              "@sn", route.GetColumn("route_short_name") |> Sql.text
              "@ln", route.GetColumn("route_long_name") |> Sql.text
              "@ty",
              NpgsqlParameter(null, enum<RouteType> (int (route.GetColumn("route_type"))))
              |> Sql.parameter
              "@aid", route.GetColumn("agency_id") |> Sql.text ]
        |> Sql.executeNonQuery
        |> ignore

    connection

let readStops (archive: ZipArchive) (connection: NpgsqlConnection) =
    use stops = archive.GetEntry("stops.txt").Open() |> CsvFile.Load

    for stop in stops.Rows do
        connection
        |> Sql.existingConnection
        |> Sql.query "INSERT INTO STOP(stopId, stopName, stopLocation) VALUES (@id, @name, @location)"
        |> Sql.parameters
            [ "@id", stop.GetColumn("stop_id") |> Sql.text
              "@name", stop.GetColumn("stop_name") |> Sql.text
              "@location",
              NpgsqlParameter(null, Point(stop.GetColumn("stop_lon") |> float, stop.GetColumn("stop_lat") |> float))
              |> Sql.parameter ]
        |> Sql.executeNonQuery
        |> ignore

    connection

let readShapePoints (archive: ZipArchive) (connection: NpgsqlConnection) =
    use shapePoints = archive.GetEntry("shapes.txt").Open() |> CsvFile.Load

    for shape in shapePoints.Rows do
        connection
        |> Sql.existingConnection
        |> Sql.query "INSERT INTO SHAPE_POINT(shapePointIndex, shapedId, shapePointLocation) VALUES (@si, @sid, @sl)"
        |> Sql.parameters
            [ "@si", int (shape.GetColumn("shape_pt_sequence")) |> Sql.int
              "@sid", shape.GetColumn("shape_id") |> Sql.text
              "@sl",
              NpgsqlParameter(
                  null,
                  Point(shape.GetColumn("shape_pt_lon") |> float, shape.GetColumn("shape_pt_lat") |> float)
              )
              |> Sql.parameter ]
        |> Sql.executeNonQuery
        |> ignore

    connection

let readCalendarDates (archive: ZipArchive) (connection: NpgsqlConnection) =
    let calendarDates = archive.GetEntry("calendar_dates.txt") |> entryLineStream

    use writer =
        connection.BeginTextImport "COPY GTFS_CALENDAR_DATES FROM STDIN DELIMITER ','"

    for line in calendarDates |> Seq.skip 1 do
        writer.WriteLine(line)

    connection

let readTrips (archive: ZipArchive) (connection: NpgsqlConnection) =
    let trips = archive.GetEntry("trips.txt") |> entryLineStream

    use writer = connection.BeginTextImport "COPY GTFS_TRIP FROM STDIN DELIMITER ','"

    for line in trips |> Seq.skip 1 do
        writer.WriteLine(line)

    connection

let readStopTimes (archive: ZipArchive) (connection: NpgsqlConnection) =
    let stopTimes = archive.GetEntry("stop_times.txt") |> entryLineStream

    use writer =
        connection.BeginTextImport "COPY GTFS_STOP_TIMES FROM STDIN DELIMITER ','"

    for line in stopTimes |> Seq.skip 1 do
        writer.WriteLine(line)

    connection

let executeNonQuery query (connection: NpgsqlConnection) =
    connection |> Sql.existingConnection |> Sql.query query |> Sql.executeNonQuery

let convertData (connection: NpgsqlConnection) =
    let queryView =
        """
CREATE TEMPORARY VIEW VALID_DATES AS
SELECT service_id, date
FROM GTFS_CALENDAR_DATES
WHERE exception_type = 1
  AND date >= TO_CHAR(CURRENT_DATE, 'yyyyMMdd')
  AND date < TO_CHAR(CURRENT_DATE + 7, 'yyyyMMdd');
"""

    connection |> executeNonQuery queryView |> ignore

    let insertTrips =
        """
INSERT
INTO TRIP(tripId, tripDirection, routeId, shapedId)
SELECT t.trip_id,
       CASE WHEN t.direction_id = 0 THEN 'one_direction' ELSE 'the_opposite' END::DIRECTION,
       t.route_id,
       t.shape_id
FROM GTFS_TRIP t
         INNER JOIN VALID_DATES gcd ON gcd.service_id = t.service_id;
"""

    connection |> executeNonQuery insertTrips |> printfn "Inserted %d trips"


    let insertStoppingAt =
        """
INSERT
INTO STOPPING_AT(tripId, stopId, stoppingAtIndex, stoppingAtPredictedTime)
SELECT st.trip_id,
       st.stop_id,
       st.stop_sequence,
       to_timestamp(gcd.date, 'yyyyMMdd') + justify_interval(st.arrival_time::interval)
FROM GTFS_STOP_TIMES st
         INNER JOIN GTFS_TRIP t on st.trip_id = t.trip_id
         INNER JOIN VALID_DATES gcd ON gcd.service_id = t.service_id
"""

    connection
    |> executeNonQuery insertStoppingAt
    |> printfn "Inserted %d stop times"

    connection
