DROP TABLE IF EXISTS chyf.nhn_workunit_temp;
DROP TABLE IF EXISTS chyf.nhn_workunit;
-- "C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe" -f "PostgreSQL" PG:"dbname='chyf' host='localhost' port='5432' user='postgres' password='sql'" -nln chyf.nhn_workunit_temp -lco GEOMETRY_NAME=geometry -nlt POLYGON C:\Users\Emily\Downloads\NHN_INDEX_WORKUNIT_LIMIT_2\NHN_INDEX_22_INDEX_WORKUNIT_LIMIT_2.shp


CREATE TABLE chyf.nhn_workunit (
	id varchar(7) NOT NULL,
	major_drainage_area varchar(500),
	sub_drainage_area varchar(500),
	sub_sub_drainage_area varchar(500),
	geometry geometry(POLYGON, 4326) NOT NULL,
	PRIMARY KEY (id)
);

CREATE INDEX nhn_workunit_polygon_idx ON chyf.nhn_workunit USING gist (polygon);

insert into chyf.nhn_workunit(id, major_drainage_area, sub_drainage_area, sub_sub_drainage_area, polygon)
select datasetnam, wscmdaname, wscsdaname,wscssdanam, st_transform(geometry, 4326)
from chyf.nhn_workunit_temp;

drop table chyf.nhn_workunit_temp;