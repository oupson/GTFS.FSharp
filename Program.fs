open System
open System.IO.Compression
open System.Net.Http
open System.IO

open GtfsReader
open Microsoft.FSharp.Core
open Npgsql
open Npgsql.FSharp
open ProtoBuf

let downloadFile url savePath =
    async {
        use client = new HttpClient()
        let! bytes = client.GetByteArrayAsync(Uri(url)) |> Async.AwaitTask
        do! File.WriteAllBytesAsync(savePath, bytes) |> Async.AwaitTask
    }

let downloadRealtime url =
    async {
        use client = new HttpClient()
        let! stream = client.GetStreamAsync(Uri(url)) |> Async.AwaitTask

        return Serializer.Deserialize<TransitRealtime.FeedMessage>(stream)
    }

let databaseUrl = Environment.GetEnvironmentVariable "DATABASE_CONNECTION_STRING"
let url = Environment.GetEnvironmentVariable "GTFS_URL"
let savePath = "/tmp/gtfs.zip"

let realtimeUrl = Environment.GetEnvironmentVariable "REALTIME_URL"

let message = downloadRealtime realtimeUrl |> Async.RunSynchronously

printfn $"Is full dataset : %b{message.Header.incrementality = TransitRealtime.FeedHeader.Incrementality.FullDataset}"

for entity in message.Entities do
    if entity.Alert <> null then
        printfn
            "Alert : %s"
            (entity.Alert.DescriptionText.Translations
             |> Seq.map (fun r -> r.Text)
             |> String.concat ",")

    if entity.TripUpdate <> null then
        printfn $"Trip update : %s{entity.TripUpdate.Trip.TripId}"

    if entity.Vehicle <> null then
        printfn $"Vehicle position : %s{entity.Vehicle.Vehicle.Id}"

let dataSourceBuilder = NpgsqlDataSourceBuilder databaseUrl
dataSourceBuilder.MapEnum<RouteType>("route_type") |> ignore
dataSourceBuilder.MapEnum<TripDirection>("direction") |> ignore
dataSourceBuilder.UseNetTopologySuite() |> ignore
let dataSource = dataSourceBuilder.Build()

let connection = dataSource |> Sql.fromDataSource |> Sql.createConnection

downloadFile url savePath |> Async.RunSynchronously

let archive = new ZipArchive(new FileStream(savePath, FileMode.Open))
let transaction = connection.BeginTransaction()

connection
|> truncateTable "TRIP"
|> truncateTable "SHAPE_POINT"
|> truncateTable "STOP"
|> truncateTable "ROUTE"
|> truncateTable "AGENCY"

|> createTemporaryTables

|> readAgencies archive
|> readRoutes archive
|> readStops archive
|> readShapePoints archive
|> readCalendarDates archive
|> readTrips archive
|> readStopTimes archive

|> convertData

|> ignore

transaction.Commit()
