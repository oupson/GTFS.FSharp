module GtfsReader

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

let truncateTable database table =
    database
    |> Sql.existingConnection
    |> Sql.query $"TRUNCATE %s{table} RESTART IDENTITY CASCADE"
    |> Sql.executeNonQuery
    |> ignore

let readAgencies (connection: NpgsqlConnection) (archive: ZipArchive) =
    let agencies = archive.GetEntry("agency.txt").Open() |> CsvFile.Load

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

let readRoutes (connection: NpgsqlConnection) (archive: ZipArchive) =
    let transaction = connection.BeginTransaction()
    let routes = archive.GetEntry("routes.txt").Open() |> CsvFile.Load

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

    transaction.Commit()

let readStops (connection: NpgsqlConnection) (archive: ZipArchive) =
    let transaction = connection.BeginTransaction()
    let stops = archive.GetEntry("stops.txt").Open() |> CsvFile.Load

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

    transaction.Commit()

let readShapePoints (connection: NpgsqlConnection) (archive: ZipArchive) =
    let shapePoints = archive.GetEntry("shapes.txt").Open() |> CsvFile.Load

    for shape in shapePoints.Rows do
        connection
        |> Sql.existingConnection
        |> Sql.query "INSERT INTO SHAPE_POINT(shapePointIndex, shapedId, shapePointLocation) VALUES (@si, @sid, @sl)"
        |> Sql.parameters
            [ "@si", int(shape.GetColumn("shape_pt_sequence")) |> Sql.int
              "@sid", shape.GetColumn("shape_id") |> Sql.text
              "@sl",
              NpgsqlParameter(null, Point(shape.GetColumn("shape_pt_lon") |> float, shape.GetColumn("shape_pt_lat") |> float))
              |> Sql.parameter ]
        |> Sql.executeNonQuery
        |> ignore
