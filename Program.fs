open System
open System.IO.Compression
open System.Net.Http
open System.IO

open GtfsReader
open Npgsql
open Npgsql.FSharp

let downloadFile url savePath =
    async {
        use client = new HttpClient()
        let! bytes = client.GetByteArrayAsync(Uri(url)) |> Async.AwaitTask
        do! File.WriteAllBytesAsync(savePath, bytes) |> Async.AwaitTask
    }

let databaseUrl = Environment.GetEnvironmentVariable "DATABASE_CONNECTION_STRING"
let url = Environment.GetEnvironmentVariable "GTFS_URL"
let savePath = "/tmp/gtfs.zip"

let dataSourceBuilder = NpgsqlDataSourceBuilder databaseUrl
dataSourceBuilder.MapEnum<RouteType>("route_type") |> ignore
dataSourceBuilder.UseNetTopologySuite() |> ignore
let dataSource = dataSourceBuilder.Build()

let connection = dataSource |> Sql.fromDataSource |> Sql.createConnection

downloadFile url savePath |> Async.RunSynchronously

truncateTable connection "STOP"
truncateTable connection "ROUTE"
truncateTable connection "AGENCY"

let archive = new ZipArchive(new FileStream(savePath, FileMode.Open))
readAgencies connection archive
readRoutes connection archive
readStops connection archive