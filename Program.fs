open System
open System.IO.Compression
open System.Net.Http
open System.IO

open FSharp.Data
open Npgsql
open Npgsql.FSharp


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

let downloadFile url savePath =
    async {
        use client = new HttpClient()
        let! bytes = client.GetByteArrayAsync(Uri(url)) |> Async.AwaitTask
        do! File.WriteAllBytesAsync(savePath, bytes) |> Async.AwaitTask
    }

let truncateTable database table =
    database
    |> Sql.existingConnection
    |> Sql.query $"TRUNCATE %s{table} RESTART IDENTITY CASCADE"
    |> Sql.executeNonQuery
    |> ignore


let databaseUrl = Environment.GetEnvironmentVariable "DATABASE_CONNECTION_STRING"
let url = "https://chouette.enroute.mobi/api/v1/datas/keolis_orleans.gtfs.zip"
let savePath = "/tmp/gtfs.zip"

let dataSourceBuilder = NpgsqlDataSourceBuilder databaseUrl
dataSourceBuilder.MapEnum<RouteType>("route_type") |> ignore
let dataSource = dataSourceBuilder.Build()

let connection = dataSource |> Sql.fromDataSource |> Sql.createConnection

downloadFile url savePath |> Async.RunSynchronously

truncateTable connection "ROUTE"
truncateTable connection "AGENCY"

let f = new FileStream(savePath, FileMode.Open)
let archive = new ZipArchive(f)

let zipArchiveEntry = archive.GetEntry("agency.txt").Open()

let agencies = CsvFile.Load zipArchiveEntry

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
