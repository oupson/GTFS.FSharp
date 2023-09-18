module GtfsReader

open System
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
    |> Sql.query "CREATE TEMPORARY TABLE CALENDAR_DATES(serviceId TEXT, date TEXT)"
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
    use calendarDates = archive.GetEntry("calendar_dates.txt").Open() |> CsvFile.Load
    let today = DateTime.Today.ToString("yyyyMMdd")
    let tomorrow = DateTime.Today.AddDays(1).ToString("yyyyMMdd")

    for dateEntry in calendarDates.Rows do
        let date = dateEntry.GetColumn("date")

        if date = today || date = tomorrow then
            connection
            |> Sql.existingConnection
            |> Sql.query "INSERT INTO CALENDAR_DATES(serviceId, date) VALUES(@sid, @d)"
            |> Sql.parameters
                [ "@sid", dateEntry.GetColumn("service_id") |> Sql.text
                  "@d", date |> Sql.text ]
            |> Sql.executeNonQuery
            |> ignore

    connection

let readTrips (archive: ZipArchive) (connection: NpgsqlConnection) =
    use trips = archive.GetEntry("trips.txt").Open() |> CsvFile.Load

    let headers = trips.Headers.Value
    let haveHeadSigns = Array.contains "trip_headsign" headers
    let haveShortNames = Array.contains "trip_short_name" headers
    let haveShapes = Array.contains "shape_id" headers

    for trip in trips.Rows do
        let insert =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT (COUNT(serviceId) = 1) as exist FROM CALENDAR_DATES WHERE serviceId = @sid"
            |> Sql.parameters [ "sid", trip.GetColumn("service_id") |> Sql.text ]
            |> Sql.executeRow (fun r -> r.bool "exist")

        if insert then
            connection
            |> Sql.existingConnection
            |> Sql.query
                "INSERT INTO TRIP(tripId, tripHeadSign, tripShortName, tripDirection, routeId, shapedId) VALUES (@tid, @ths, @tsn, @td, @ri, @si)"
            |> Sql.parameters
                [ "@tid", trip.GetColumn("trip_id") |> Sql.text
                  "@ths",
                  if haveHeadSigns then
                      trip.GetColumn("trip_headsign") |> Sql.text
                  else
                      Sql.dbnull
                  "@tsn",
                  if haveShortNames then
                      trip.GetColumn("trip_short_name") |> Sql.text
                  else
                      Sql.dbnull
                  "@td",
                  NpgsqlParameter(null, enum<TripDirection> (int (trip.GetColumn("direction_id"))))
                  |> Sql.parameter
                  "@ri", trip.GetColumn("route_id") |> Sql.text
                  "@si",
                  if haveShapes then
                      trip.GetColumn("shape_id") |> Sql.text
                  else
                      Sql.dbnull ]
            |> Sql.executeNonQuery
            |> ignore

    connection
