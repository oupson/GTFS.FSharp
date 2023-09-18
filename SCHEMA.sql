DROP TABLE IF EXISTS VEHICLE;
DROP TABLE IF EXISTS STOPPING_AT;
DROP TABLE IF EXISTS TRIP;
DROP TABLE IF EXISTS STOP;
DROP TABLE IF EXISTS SHAPE_POINT;
DROP TABLE IF EXISTS ROUTE;
DROP TABLE IF EXISTS AGENCY;

DROP TYPE IF EXISTS ROUTE_TYPE;
DROP TYPE IF EXISTS DIRECTION;

CREATE TABLE AGENCY
(
    agencyId   TEXT NOT NULL,
    agencyName TEXT NOT NULL,
    agencyUrl  TEXT NULL,
    CONSTRAINT PK_AGENCY PRIMARY KEY (agencyId)
);

CREATE TYPE ROUTE_TYPE AS ENUM ('tram', 'subway','rail','bus','ferry','cabletram','aeriallift','funicular','trolleybus','monorail');

CREATE TABLE ROUTE
(
    routeId        TEXT       NOT NULL,
    routeShortName TEXT       NULL,
    routeLongName  TEXT       NULL,
    routeType      ROUTE_TYPE NOT NULL,
    agencyId       TEXT       NOT NULL,
    CONSTRAINT PK_ROUTE PRIMARY KEY (routeid),
    CONSTRAINT CHECK_ROUTE_NAME_NOT_NULL CHECK ( ROUTE.routeShortName IS NOT NULL OR ROUTE.routeLongName IS NOT NULL )
);

CREATE TABLE SHAPE_POINT
(
    shapePointIndex    INTEGER                NOT NULL,
    shapedId           TEXT                   NOT NULL,
    shapePointLocation GEOGRAPHY(POINT, 4326) NOT NULL,
    CONSTRAINT PK_SHAPE_POINT PRIMARY KEY (shapedId, shapePointIndex)
);

CREATE TABLE STOP
(
    stopId       TEXT                   NOT NULL,
    stopName     TEXT                   NOT NULL,
    stopLocation GEOGRAPHY(POINT, 4326) NOT NULL,
    CONSTRAINT PK_STOP PRIMARY KEY (stopId)
);

CREATE TABLE STOPPING_AT
(
    tripId                  TEXT      NOT NULL,
    stopId                  TEXT      NOT NULL,
    stoppingAtIndex         INTEGER   NOT NULL,
    stoppingAtPredictedTime TIMESTAMP NULL,
    stoppingAtRealTime      TIMESTAMP NULL,
    stoppingAtIsSkipped     BOOLEAN   NOT NULL DEFAULT FALSE,
    CONSTRAINT PK_STOPPING_AT PRIMARY KEY (tripId, stopId),
    CONSTRAINT CHECK_STOPPING_AT_HAVE_TIME CHECK ( stoppingAtPredictedTime IS NOT NULL OR stoppingAtRealTime IS NOT NULL )
);

CREATE TYPE DIRECTION AS ENUM ('one_direction', 'the_opposite');

CREATE TABLE TRIP
(
    tripId         TEXT      NOT NULL,
    tripHeadSign   TEXT      NULL,
    tripShortName  TEXT      NULL,
    tripDirection  DIRECTION NULL,
    tripIsCanceled BOOLEAN   NOT NULL DEFAULT FALSE,
    routeId        TEXT      NOT NULL,
    shapedId       TEXT      NOT NULL,
    CONSTRAINT PK_TRIP PRIMARY KEY (tripId)
);

CREATE TABLE VEHICLE
(
    vehicleId       TEXT NOT NULL,
    vehiclePosition GEOMETRY(POINT, 4326),
    tripId          TEXT NOT NULL,
    CONSTRAINT PK_VEHICLE PRIMARY KEY (vehicleId)
);

ALTER TABLE ROUTE
    ADD CONSTRAINT FK_ROUTE_AGENCY FOREIGN KEY (agencyId) REFERENCES AGENCY (agencyId);
ALTER TABLE STOPPING_AT
    ADD CONSTRAINT FK_STOPPING_AT_STOP FOREIGN KEY (stopId) REFERENCES STOP (stopId);
ALTER TABLE STOPPING_AT
    ADD CONSTRAINT FK_STOPPING_AT_TRIP FOREIGN KEY (tripId) REFERENCES TRIP (tripId);
ALTER TABLE TRIP
    ADD CONSTRAINT FK_TRIP_ROUTE FOREIGN KEY (routeId) REFERENCES ROUTE (routeId);
ALTER TABLE VEHICLE
    ADD CONSTRAINT FK_VEHICLE_TRIP FOREIGN KEY (tripId) REFERENCES TRIP (tripId);