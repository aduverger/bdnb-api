---------------------------------
-- Copyright (C) 2021-2022 CARTO
---------------------------------

CREATE OR REPLACE PROCEDURE carto.__CREATE_DROP_TABLE
(schema_name VARCHAR(MAX))
AS $$
DECLARE
  row RECORD;
BEGIN
  DROP TABLE IF EXISTS _udfs_info;
  --open cur refcursor;
  CREATE TEMP TABLE _udfs_info (f_oid BIGINT, f_kind VARCHAR(1), f_name VARCHAR(MAX), arg_index BIGINT, f_argtype VARCHAR(MAX));
  FOR row IN SELECT oid::BIGINT f_oid, kind::VARCHAR(1) f_kind, proname::VARCHAR(MAX) f_name, i arg_index, format_type(arg_types[i-1], null)::VARCHAR(MAX) f_argtype
    FROM (
      SELECT oid, kind, proname, generate_series(1, arg_count) AS i, arg_types
      FROM (
        SELECT p.prooid oid, p.prokind kind, proname, proargtypes arg_types, pronargs arg_count
        FROM pg_catalog.pg_namespace n
        JOIN PG_PROC_INFO p
        ON  pronamespace = n.oid
        WHERE nspname = schema_name
      ) t
    ) t
  LOOP
    INSERT INTO _udfs_info(f_oid, f_kind, f_name,arg_index,f_argtype) VALUES (row.f_oid, row.f_kind, row.f_name, row.arg_index, row.f_argtype);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE carto.__DROP_FUNCTIONS
(schema_name VARCHAR(MAX))
AS $$
DECLARE
	row RECORD;
BEGIN
	CALL carto.__CREATE_DROP_TABLE(schema_name);

	FOR row IN SELECT drop_command
	FROM (
    SELECT 'DROP ' || CASE f_kind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END || ' ' || schema_name || '.' || f_name || '(' || listagg(f_argtype,',' ) WITHIN GROUP (ORDER BY arg_index) || ');' AS drop_command
    FROM _udfs_info
    GROUP BY f_oid, f_name, f_kind
  )
  LOOP
		execute row.drop_command;
	END LOOP;

	DROP TABLE IF EXISTS _udfs_info;
END;
$$ LANGUAGE plpgsql;

CALL carto.__DROP_FUNCTIONS('carto');

CREATE OR REPLACE FUNCTION carto.__CLUSTERKMEANSTABLE
(geom VARCHAR(MAX), numberofClusters INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.clustering import clusterkmeanstable
    if geom is None:
        return None
    return clusterkmeanstable(geom, numberofClusters)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE PROCEDURE carto.CREATE_CLUSTERKMEANS
(
    input VARCHAR(MAX),
    output_table INOUT VARCHAR(MAX),
    geom_column VARCHAR(MAX),
    number_of_clusters INT
)
AS $$
DECLARE
    input_query VARCHAR(MAX);
    table_format INTEGER;
    temp_table VARCHAR(MAX) := '';
    output_first VARCHAR(MAX);
    output_second VARCHAR(MAX);
    output_third VARCHAR(MAX);
    output_fourth VARCHAR(MAX);
    clustering VARCHAR (MAX);
    input_points VARCHAR(MAX);
BEGIN
    input_query := input;
    EXECUTE 'SELECT regexp_count(''' || input || ''', ''\\\\s'')' INTO table_format;
    IF table_format > 0
    THEN
        input_query := '(' || input || ')';
    END IF;
        EXECUTE 'SELECT split_part(''' || output_table || ''', ''.'', 1)' INTO output_first;
    EXECUTE 'SELECT split_part(''' || output_table || ''', ''.'', 2)' INTO output_second;
    EXECUTE 'SELECT split_part(''' || output_table || ''', ''.'', 3)' INTO output_third;
    EXECUTE 'SELECT split_part(''' || output_table || ''', ''.'', 4)' INTO output_fourth;
    IF output_first = '' OR output_second = '' OR output_fourth != ''
    THEN
        output_table := 'Invalid output table name. It must have the form [DATABASE.]SCHEMA.TABLE';
        RAISE INFO 'Invalid output table name. It must have the form [DATABASE.]SCHEMA.TABLE';
        RETURN;
    END IF;

        EXECUTE 'CREATE TABLE ' || output_table || ' AS
        SELECT *,
        ROW_NUMBER() OVER() AS __carto_idx,
        NULL::INT AS cluster_id
        FROM ' || input_query;

        EXECUTE 'WITH input_points AS (
                SELECT __carto_idx, ST_X(' || geom_column || ')::DECIMAL(12,7) || '','' ||
                      ST_Y(' || geom_column || ')::DECIMAL(12,7) AS coordinates
                FROM ' || output_table || ' WHERE ' || geom_column || ' IS NOT NULL
            )
            SELECT ''{"_coords":['' || LISTAGG(coordinates, '','')
            WITHIN GROUP (ORDER BY __carto_idx ASC) || '']}'' FROM input_points' INTO input_points;

     EXECUTE 'SELECT carto.__CLUSTERKMEANSTABLE(''' ||
           input_points || ''',' || number_of_clusters ||')' INTO clustering;

         EXECUTE 'CREATE TEMP TABLE DUAL AS SELECT 0 AS DUMMY';

        EXECUTE 'UPDATE ' || output_table || '
            SET cluster_id = g.c::INT
            FROM (
                SELECT c.i, c.c
                FROM (
                    SELECT JSON_PARSE(''' || clustering || ''') cluster_arr
                    FROM DUAL
                ) as cs, cs.cluster_arr as c
            ) g
            WHERE g.i = __carto_idx';

        EXECUTE 'DROP TABLE IF EXISTS DUAL';
    EXECUTE 'ALTER TABLE ' || output_table || ' DROP COLUMN __carto_idx';

    output_table := 'Table ' || output_table || ' created with the clustering';

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION carto.__CLUSTERKMEANS
(geom VARCHAR(MAX), numberofClusters INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.clustering import clusterkmeans
    if geom is None:
        return None
    return clusterkmeans(geom, numberofClusters)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_CLUSTERKMEANS
(GEOMETRY)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__CLUSTERKMEANS(ST_ASGEOJSON($1), SQRT(ST_NPoints($1)/2)::INT))
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_CLUSTERKMEANS
(GEOMETRY, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__CLUSTERKMEANS(ST_ASGEOJSON($1), $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__BEZIERSPLINE
(linestring VARCHAR(MAX), resolution INT, sharpness FLOAT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.constructors import bezier_spline

    if linestring is None or resolution is None or sharpness is None:
        return None

    return bezier_spline(linestring, resolution, sharpness)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_BEZIERSPLINE
(GEOMETRY)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__BEZIERSPLINE(ST_ASGEOJSON($1)::VARCHAR(MAX), 10000, 0.85)
    $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_BEZIERSPLINE
(GEOMETRY, INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__BEZIERSPLINE(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, 0.85)
    $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_BEZIERSPLINE
(GEOMETRY, INT, FLOAT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__BEZIERSPLINE(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3)
    $$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__MAKEELLIPSE
(
    center VARCHAR(MAX),
    xSemiAxis FLOAT8,
    ySemiAxis FLOAT8,
    angle FLOAT8,
    units VARCHAR(10),
    steps INT
)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.constructors import ellipse

    if center is None or xSemiAxis is None or ySemiAxis is None or angle is None or units is None or steps is None:
        return None

    geom_options = {}
    geom_options['angle'] = angle
    geom_options['steps'] = steps
    geom_options['units'] = units
    return ellipse(
        center=center,
        x_semi_axis=xSemiAxis,
        y_semi_axis=ySemiAxis,
        options=geom_options,
    )
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_MAKEELLIPSE
(GEOMETRY, FLOAT8, FLOAT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__MAKEELLIPSE(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3, 0, 'kilometers', 64)
    $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_MAKEELLIPSE
(GEOMETRY, FLOAT8, FLOAT8, FLOAT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__MAKEELLIPSE(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3, $4, 'kilometers', 64)
    $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_MAKEELLIPSE
(GEOMETRY, FLOAT8, FLOAT8, FLOAT8, VARCHAR(10))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__MAKEELLIPSE(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3, $4, $5, 64)
    $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_MAKEELLIPSE
(GEOMETRY, FLOAT8, FLOAT8, FLOAT8, VARCHAR(10), INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__MAKEELLIPSE(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3, $4, $5, $6)
    $$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_MAKEENVELOPE
(FLOAT8, FLOAT8, FLOAT8, FLOAT8)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT('POLYGON((' || $1 || ' ' || $2 || ',' || $1 || ' ' || $4 || ',' || $3 || ' ' || $4 || ',' || $3 || ' ' || $2 || ',' || $1 || ' ' || $2 || '))')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_BOUNDARY
(quadbin BIGINT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import cell_to_bounding_box

    if quadbin is None:
        return None

    bbox = cell_to_bounding_box(quadbin)
    return 'POLYGON(({west} {south},{west} {north},{east} {north},{east} {south},{west} {south}))'.format(west=bbox[0], south=bbox[1], east=bbox[2], north=bbox[3])
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_BOUNDARY
(BIGINT)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__QUADBIN_BOUNDARY($1), 4326)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADBIN_FROMZXY
(z BIGINT, x BIGINT, y BIGINT)
RETURNS BIGINT
IMMUTABLE
AS $$
    if z is None or x is None or y is None:
        raise Exception('NULL argument passed to UDF')

    from carto_analytics_toolbox_core.quadbin import tile_to_cell

    return tile_to_cell((x, y, z))
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.ST_TILEENVELOPE
(INT, INT, INT)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT carto.QUADBIN_BOUNDARY(carto.QUADBIN_FROMZXY($1, $2, $3))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__GEOJSONTOWKT
(geom VARCHAR(MAX))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.transformations import wkt_from_geojson

    return wkt_from_geojson(geom)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.__ST_GEOMFROMGEOJSON
(VARCHAR(MAX))
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__GEOJSONTOWKT($1), 4326)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.PLACEKEY_ASH3
(placekey VARCHAR(19))
RETURNS VARCHAR
STABLE
AS $$
    from carto_analytics_toolbox_core.placekey import placekey_to_h3, placekey_is_valid

    if not placekey_is_valid(placekey):
        return None
    return placekey_to_h3(placekey)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.PLACEKEY_FROMH3
(h3_index VARCHAR(15))
RETURNS VARCHAR
STABLE
AS $$
    from carto_analytics_toolbox_core.placekey import h3_to_placekey, h3_is_valid
    
    if not h3_is_valid(h3_index):
        return None
    return h3_to_placekey(h3_index)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.PLACEKEY_ISVALID
(placekey VARCHAR(19))
RETURNS BOOLEAN
STABLE
AS $$
    from carto_analytics_toolbox_core.placekey import placekey_is_valid

    return placekey_is_valid(placekey)

$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__DELAUNAYGENERIC
(points VARCHAR(MAX), delaunay_type VARCHAR(15))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.processing import PRECISION
    import geojson
    import json
    from scipy.spatial import Delaunay

    if points is None:
        return None

    if delaunay_type != 'lines' and delaunay_type != 'poly':
        return None
 
    # Take the type of geometry
    _geom = json.loads(points)
    _geom['precision'] = PRECISION
    geom = json.dumps(_geom)
    geom = geojson.loads(geom)
    
    coords = []
    if geom.type != 'MultiPoint':
        raise Exception('Invalid operation: Input points parameter must be MultiPoint.')
    else:
        coords = list(geojson.utils.coords(geom))

    tri = Delaunay(coords)

    lines = []
    for triangle in tri.simplices:
        p_1 = coords[triangle[0]]
        p_2 = coords[triangle[1]]
        p_3 = coords[triangle[2]]
        if delaunay_type == 'lines':
            lines.append([p_1, p_2, p_3, p_1])
        else:
            lines.append([[p_1, p_2, p_3, p_1]])

            
    if delaunay_type == 'lines':
        return str(geojson.MultiLineString(lines, precision=PRECISION))
    else:
        return str(geojson.MultiPolygon(lines, precision=PRECISION))

$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.ST_DELAUNAYLINES
(GEOMETRY)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__DELAUNAYGENERIC(ST_ASGEOJSON($1)::VARCHAR(MAX), 'lines')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_DELAUNAYPOLYGONS
(GEOMETRY)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__DELAUNAYGENERIC(ST_ASGEOJSON($1)::VARCHAR(MAX), 'poly')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_POLYGONIZE
(GEOMETRY)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_MAKEPOLYGON($1)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__VORONOIGENERIC
(points VARCHAR(MAX), bbox VARCHAR(MAX), voronoi_type VARCHAR(15))
RETURNS VARCHAR(MAX)
STABLE
AS $$ 
    from carto_analytics_toolbox_core.processing import voronoi_generic, PRECISION
    import geojson
    import json
    
    bbox_array = []
    if bbox is not None:
        bbox_array = json.loads(bbox)

    if points is None:
        return None

    if voronoi_type != 'lines' and voronoi_type != 'poly':
        return None

    if bbox is not None and len(bbox_array) != 4:
        return None

    _geom = json.loads(points)
    _geom['precision'] = PRECISION
    geom_geojson = json.dumps(_geom)
    geom_geojson = geojson.loads(geom_geojson)

    return str(voronoi_generic(geom_geojson, bbox_array, voronoi_type))

$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.ST_VORONOILINES
(GEOMETRY, SUPER)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__VORONOIGENERIC(ST_ASGEOJSON($1)::VARCHAR(MAX), JSON_SERIALIZE($2)::VARCHAR(MAX), 'lines')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_VORONOILINES
(GEOMETRY)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__VORONOIGENERIC(ST_ASGEOJSON($1)::VARCHAR(MAX), NULL, 'lines')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_VORONOIPOLYGONS
(GEOMETRY, SUPER)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__VORONOIGENERIC(ST_ASGEOJSON($1)::VARCHAR(MAX), JSON_SERIALIZE($2)::VARCHAR(MAX), 'poly')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_VORONOIPOLYGONS
(GEOMETRY)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__VORONOIGENERIC(ST_ASGEOJSON($1)::VARCHAR(MAX), NULL, 'poly')
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_BBOX
(quadbin BIGINT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import cell_to_bounding_box
    import json

    if quadbin is None:
        return None

    return json.dumps(cell_to_bounding_box(quadbin))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_BBOX
(BIGINT)
RETURNS SUPER
STABLE
AS $$
    SELECT JSON_PARSE(carto.__QUADBIN_BBOX($1))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_CENTER
(quadbin BIGINT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import cell_to_point

    if quadbin is None:
        return None

    (x,y) = cell_to_point(quadbin)
    return 'POINT ({} {})'.format(x,y)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_CENTER
(BIGINT)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__QUADBIN_CENTER($1), 4326)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADBIN_FROMLONGLAT
(longitude FLOAT8, latitude FLOAT8, resolution INT)
RETURNS BIGINT
IMMUTABLE
AS $$
    if longitude is None or latitude is None or resolution is None:
        return None

    if resolution < 0 or resolution > 26:
        raise Exception('Invalid resolution: should be between 0 and 26')

    from carto_analytics_toolbox_core.quadbin import point_to_cell

    return point_to_cell(longitude, latitude, resolution)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.QUADBIN_FROMGEOGPOINT
(GEOMETRY, INT)
RETURNS BIGINT
STABLE
AS $$
    SELECT CASE ST_SRID($1)
        WHEN 0 THEN carto.QUADBIN_FROMLONGLAT(ST_X(ST_SETSRID($1, 4326)), ST_Y(ST_SETSRID($1, 4326)), $2)
        ELSE carto.QUADBIN_FROMLONGLAT(ST_X(ST_TRANSFORM($1, 4326)), ST_Y(ST_TRANSFORM($1, 4326)), $2)
    END
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADBIN_ISVALID
(quadbin BIGINT)
RETURNS BOOLEAN
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import is_valid_cell

    if quadbin is None:
        return False

    return is_valid_cell(quadbin)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_KRING
(origin BIGINT, size INT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import k_ring
    import json

    if origin is None or origin <= 0:
        raise Exception('Invalid input origin')

    if size is None or size < 0:
        raise Exception('Invalid input size')

    return json.dumps(k_ring(origin, size))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_KRING
(BIGINT, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT JSON_PARSE(carto.__QUADBIN_KRING($1, $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_KRING_DISTANCES
(origin BIGINT, size INT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import k_ring_distances
    import json

    if origin is None or origin <= 0:
        raise Exception('Invalid input origin')

    if size is None or size < 0:
        raise Exception('Invalid input size')

    return json.dumps(k_ring_distances(origin, size))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_KRING_DISTANCES
(BIGINT, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT JSON_PARSE(carto.__QUADBIN_KRING_DISTANCES($1, $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_POLYFILL
(geojson VARCHAR(MAX), resolution INT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import geometry_to_cells
    import json

    if geojson is None or resolution is None:
        return None

    if resolution < 0 or resolution > 26:
        raise Exception('Invalid resolution, should be between 0 and 26')

    quadbins = geometry_to_cells(geojson, resolution)

    return json.dumps(quadbins)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_POLYFILL
(GEOMETRY, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT CASE ST_SRID($1)
        WHEN 0 THEN JSON_PARSE(carto.__QUADBIN_POLYFILL(ST_ASGEOJSON(ST_SETSRID($1, 4326))::VARCHAR(MAX), $2))
        ELSE JSON_PARSE(carto.__QUADBIN_POLYFILL(ST_ASGEOJSON(ST_TRANSFORM($1, 4326))::VARCHAR(MAX), $2))
    END
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADBIN_RESOLUTION
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT ($1 >> 52) & CAST(31 AS BIGINT)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADBIN_SIBLING
(quadbin BIGINT, direction VARCHAR)
RETURNS BIGINT
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import cell_sibling

    if quadbin is None or direction is None:
        return None

    return cell_sibling(quadbin, direction)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOCHILDREN
(quadbin BIGINT, resolution INT)
RETURNS VARCHAR(MAX)
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import cell_to_children
    import json

    if quadbin is None or resolution is None:
        raise Exception('NULL argument passed to UDF')

    return json.dumps(cell_to_children(quadbin, resolution))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADBIN_TOCHILDREN
(BIGINT, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT JSON_PARSE(carto.__QUADBIN_TOCHILDREN($1, $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADBIN_TOPARENT
(BIGINT, INT)
RETURNS BIGINT
STABLE
AS $$
  SELECT ($1 & ~(CAST(31 AS BIGINT) << 52)) | (CAST($2 AS BIGINT) << 52) | (4503599627370495 >> ($2 << 1))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_XY_INTERLEAVED5
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT ($1 | ($1 >> 16)) & CAST(FROM_HEX('00000000FFFFFFFF') AS BIGINT)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_XY_INTERLEAVED4
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_XY_INTERLEAVED5(
        ($1 | ($1 >> 8)) & CAST(FROM_HEX('0000FFFF0000FFFF') AS BIGINT)
        )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_XY_INTERLEAVED3
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_XY_INTERLEAVED4(
        ($1 | ($1 >> 4)) & CAST(FROM_HEX('00FF00FF00FF00FF') AS BIGINT)
        )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_XY_INTERLEAVED2
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_XY_INTERLEAVED3(
        ($1 | ($1 >> 2)) & CAST(FROM_HEX('0F0F0F0F0F0F0F0F') AS BIGINT)
        )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_XY_INTERLEAVED1
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_XY_INTERLEAVED2(
        ($1 | ($1 >> 1)) & CAST(FROM_HEX('3333333333333333') AS BIGINT)
        )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_XY_PREINTERLEAVED
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_XY_INTERLEAVED1(
        $1 & CAST(FROM_HEX('5555555555555555') AS BIGINT)
        )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_X
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_XY_PREINTERLEAVED(
        ($1 & CAST(FROM_HEX('00FFFFFFFFFFFFF') AS BIGINT)) << 12
        )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.__QUADBIN_TOZXY_Y
(BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT carto.__QUADBIN_TOZXY_X($1 >> 1)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.QUADBIN_TOZXY
(BIGINT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse('{' ||
        '"z": ' || carto.QUADBIN_RESOLUTION($1) || ',' ||
        '"x": ' || (carto.__QUADBIN_TOZXY_X($1) >> (32 - CAST(carto.QUADBIN_RESOLUTION($1) AS INT))) || ',' ||
        '"y": ' || (carto.__QUADBIN_TOZXY_Y($1) >> (32 - CAST(carto.QUADBIN_RESOLUTION($1) AS INT))) || '}'
        )
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_FROMQUADINT
(quadint BIGINT)
RETURNS BIGINT
IMMUTABLE
AS $$
    from carto_analytics_toolbox_core.quadbin import tile_to_cell

    z = quadint & 31
    x = (quadint >> 5) & ((1 << z) - 1)
    y = quadint >> (z + 5)

    return tile_to_cell((x, y, z))
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_INT_TOSTRING
(BIGINT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT TO_HEX($1)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADBIN_STRING_TOINT
(VARCHAR(MAX))
RETURNS BIGINT
STABLE
AS $$
    SELECT CAST(FROM_HEX($1) AS BIGINT)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADINT_BBOX
(quadint BIGINT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import bbox

    if quadint is None:
        raise Exception('NULL argument passed to UDF')

    return str(bbox(quadint))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADINT_BBOX
(BIGINT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__QUADINT_BBOX($1))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADINT_BOUNDARY
(quadint BIGINT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import quadint_to_geojson
    import json

    if quadint is None:
        raise Exception('NULL argument passed to UDF')

    geojson = quadint_to_geojson(quadint)['geometry']
    return json.dumps(geojson)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADINT_BOUNDARY
(BIGINT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    SELECT carto.__QUADINT_BOUNDARY($1)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADINT_FROMLONGLAT
(longitude FLOAT8, latitude FLOAT8, resolution INT)
RETURNS BIGINT
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import quadint_from_location

    if longitude is None or latitude is None or resolution is None:
        raise Exception('NULL argument passed to UDF')

    return quadint_from_location(longitude, latitude, resolution)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.QUADINT_FROMGEOGPOINT
(GEOMETRY, INT)
RETURNS BIGINT
STABLE
AS $$
    SELECT CASE ST_SRID($1)
        WHEN 0 THEN carto.QUADINT_FROMLONGLAT(ST_X(ST_SetSRID($1, 4326)), ST_Y(ST_SetSRID($1, 4326)), $2)
        ELSE carto.QUADINT_FROMLONGLAT(ST_X(ST_TRANSFORM($1, 4326)), ST_Y(ST_TRANSFORM($1, 4326)), $2)
    END
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADINT_FROMQUADKEY
(quadkey VARCHAR)
RETURNS BIGINT
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import quadint_from_quadkey
    return quadint_from_quadkey(quadkey)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.QUADINT_FROMZXY
(INT, INT, INT)
RETURNS BIGINT
STABLE
AS $$
    SELECT ($1::BIGINT & 31) | ($2::BIGINT << 5) | ($3::BIGINT << ($1 + 5))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADINT_KRING
(origin BIGINT, size INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import kring

    if origin is None or origin <= 0:
        raise Exception('Invalid input origin')

    if size is None or size < 0:
        raise Exception('Invalid input size')

    return str(kring(origin, size))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADINT_KRING
(BIGINT, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__QUADINT_KRING($1, $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADINT_KRING_DISTANCES
(origin BIGINT, size INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import kring_distances
    import json

    if origin is None or origin <= 0:
        raise Exception('Invalid input origin')

    if size is None or size < 0:
        raise Exception('Invalid input size')

    return json.dumps(kring_distances(origin, size))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADINT_KRING_DISTANCES
(BIGINT, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__QUADINT_KRING_DISTANCES($1, $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADINT_POLYFILL
(geojson VARCHAR(MAX), resolution INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import geojson_to_quadints
    import json

    if geojson is None or resolution is None:
        raise Exception('NULL argument passed to UDF')

    pol = json.loads(geojson)
    quadints = []
    if pol['type'] == 'GeometryCollection':
        for geom in pol['geometries']:
            quadints += geojson_to_quadints(
                geom, {'min_zoom': resolution, 'max_zoom': resolution}
            )
        quadints = list(set(quadints))
    else:
        quadints = geojson_to_quadints(
            pol, {'min_zoom': resolution, 'max_zoom': resolution}
        )

    return str(quadints)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADINT_POLYFILL
(GEOMETRY, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT CASE ST_SRID($1)
        WHEN 0 THEN json_parse(carto.__QUADINT_POLYFILL(ST_ASGEOJSON(ST_SetSRID($1, 4326))::VARCHAR(MAX), $2))
        ELSE json_parse(carto.__QUADINT_POLYFILL(ST_ASGEOJSON(ST_TRANSFORM($1, 4326))::VARCHAR(MAX), $2))
    END
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADINT_SIBLING
(quadint BIGINT, direction VARCHAR)
RETURNS BIGINT
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import sibling

    if quadint is None or direction is None:
        raise Exception('NULL argument passed to UDF')

    return sibling(quadint, direction)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__QUADINT_TOCHILDREN
(quadint BIGINT, resolution INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import to_children

    if quadint is None or resolution is None:
        raise Exception('NULL argument passed to UDF')

    return str(to_children(quadint, resolution))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.QUADINT_TOCHILDREN
(BIGINT, INT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__QUADINT_TOCHILDREN($1, $2))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.QUADINT_TOPARENT
(quadint BIGINT, resolution INT)
RETURNS BIGINT
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import to_parent

    if quadint is None or resolution is None:
        raise Exception('NULL argument passed to UDF')

    return to_parent(quadint, resolution)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.QUADINT_TOQUADKEY
(quadint BIGINT)
RETURNS VARCHAR
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import quadkey_from_quadint

    if quadint is None:
        raise Exception('NULL argument passed to UDF')

    return quadkey_from_quadint(quadint)
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.QUADINT_TOZXY
(BIGINT)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse('{' ||
        '"z": ' || ($1 & 31) || ',' ||
        '"x": ' || (($1 >> 5) & ((1 << ($1 & 31)::INT) - 1)) || ',' ||
        '"y": ' || (($1 >> (5 + ($1 & 31)::INT))) || '}'
        )
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADINT_RESOLUTION
(index BIGINT)
RETURNS BIGINT
STABLE
AS $$
    SELECT CAST($1 AS BIGINT) & CAST(31 AS BIGINT)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__QUADKEY_BBOX_INTERNAL
(quadkey VARCHAR(MAX))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.quadkey import bbox_quadkey

    if quadkey is None:
        raise Exception('NULL argument passed to UDF')

    return str(bbox_quadkey(quadkey))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.__QUADKEY_BBOX
(VARCHAR(MAX))
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__QUADKEY_BBOX_INTERNAL($1))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__GENERATEPOINTS
(geom VARCHAR(MAX), npoints INT)
RETURNS VARCHAR(MAX)
VOLATILE
AS $$
    from carto_analytics_toolbox_core.random import generatepoints
    return generatepoints(geom, npoints)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_GENERATEPOINTS
(GEOMETRY, INT)
RETURNS VARCHAR(MAX)
VOLATILE
AS $$
    SELECT carto.__GENERATEPOINTS(ST_ASGEOJSON($1), $2)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.S2_BOUNDARY
(id INT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import get_cell_boundary

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return get_cell_boundary(id)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_FROMLONGLAT
(longitude FLOAT8, latitude FLOAT8, resolution INT4)
RETURNS INT8
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import longlat_as_int64_id

    if longitude is None or latitude is None or resolution is None:
        raise Exception('NULL argument passed to UDF')
    
    return longlat_as_int64_id(longitude, latitude, resolution)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_FROMGEOGPOINT
(point GEOMETRY, resolution INT4)
RETURNS INT8
STABLE
AS $$
    SELECT carto.S2_FROMLONGLAT(ST_X($1)::FLOAT8, ST_Y($1)::FLOAT8, $2)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.S2_FROMHILBERTQUADKEY
(hquadkey VARCHAR(MAX))
RETURNS INT8
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import hilbert_quadkey_to_id

    if hquadkey is None:
        raise Exception('NULL argument passed to UDF')
    
    return hilbert_quadkey_to_id(hquadkey)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_FROMTOKEN
(token VARCHAR(MAX))
RETURNS INT8
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import token_to_int64_id

    if token is None:
        raise Exception('NULL argument passed to UDF')
    
    return token_to_int64_id(token)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_FROMUINT64REPR
(uid VARCHAR(MAX))
RETURNS INT8
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import uint64_to_int64

    if uid is None:
        raise Exception('NULL argument passed to UDF')
    
    return uint64_to_int64(int(uid))
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__S2_POLYFILL_BBOX
(min_longitude FLOAT8, max_longitude FLOAT8, min_latitude FLOAT8,
 max_latitude FLOAT8, min_resolution INT4, max_resolution INT4)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import polyfill_bbox

    to_check = [min_longitude, max_longitude, min_latitude,
                max_latitude, min_resolution, max_resolution]
    for arg in to_check:
        if arg is None:
            raise Exception('NULL argument passed to UDF')
    
    return polyfill_bbox(min_longitude, max_longitude, min_latitude,
                         max_latitude, min_resolution, max_resolution)
    
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.__S2_POLYFILL_BBOX
(min_longitude FLOAT8, max_longitude FLOAT8, min_latitude FLOAT8,
 max_latitude FLOAT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import polyfill_bbox

    to_check = [min_longitude, max_longitude, min_latitude, max_latitude]
    for arg in to_check:
        if arg is None:
            raise Exception('NULL argument passed to UDF')
    
    
    return polyfill_bbox(min_longitude, max_longitude, min_latitude, max_latitude)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_POLYFILL_BBOX
(min_longitude FLOAT8, max_longitude FLOAT8, min_latitude FLOAT8,
 max_latitude FLOAT8, min_resolution INT4, max_resolution INT4)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__S2_POLYFILL_BBOX($1, $2, $3, $4, $5, $6))
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.S2_POLYFILL_BBOX
(min_longitude FLOAT8, max_longitude FLOAT8, min_latitude FLOAT8,
 max_latitude FLOAT8)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__S2_POLYFILL_BBOX($1, $2, $3, $4))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.S2_RESOLUTION
(id INT8)
RETURNS INT4
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import get_resolution

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return get_resolution(id)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__S2_TOCHILDREN
(id INT8, resolution INT4)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import to_children

    if id is None or resolution is None:
        raise Exception('NULL argument passed to UDF')
    
    return to_children(id, resolution)
    
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.__S2_TOCHILDREN
(id INT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import to_children

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return to_children(id)
    
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.S2_TOCHILDREN
(INT8, INT4)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__S2_TOCHILDREN($1, $2))
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.S2_TOCHILDREN
(INT8)
RETURNS SUPER
STABLE
AS $$
    SELECT json_parse(carto.__S2_TOCHILDREN($1))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.S2_TOHILBERTQUADKEY
(id INT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import id_to_hilbert_quadkey

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return id_to_hilbert_quadkey(id)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_TOPARENT
(id INT8, resolution INT4)
RETURNS INT8
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import to_parent

    if id is None or resolution is None:
        raise Exception('NULL argument passed to UDF')
    
    return to_parent(id, resolution)
    
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.S2_TOPARENT
(id INT8)
RETURNS INT8
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import to_parent

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return to_parent(id)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_TOTOKEN
(id INT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import int64_id_to_token

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return int64_id_to_token(id)
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.S2_TOUINT64REPR
(id INT8)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.s2 import uint64_repr_from_id

    if id is None:
        raise Exception('NULL argument passed to UDF')
    
    return str(uint64_repr_from_id(id))
    
$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.__CENTERMEAN
(geom VARCHAR(MAX))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.transformations import center_mean, PRECISION, wkt_from_geojson
    import geojson
    import json

    if geom is None:
        return None
    
    _geom = json.loads(geom)
    _geom['precision'] = PRECISION
    geojson_geom = json.dumps(_geom)
    geojson_geom = geojson.loads(geojson_geom)
    geojson_str = str(center_mean(geojson_geom))
    
    return wkt_from_geojson(geojson_str)

$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_CENTERMEAN
(GEOMETRY)
RETURNS GEOMETRY
STABLE
AS $$

    SELECT ST_GEOMFROMTEXT(carto.__CENTERMEAN(ST_ASGEOJSON($1)::VARCHAR(MAX)))
    
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__CENTERMEDIAN
(geom VARCHAR(MAX), n_iter INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.transformations import center_median, PRECISION, wkt_from_geojson
    import geojson
    import json
    
    if geom is None or n_iter is None:
        return None

    _geom = json.loads(geom)
    _geom['precision'] = PRECISION
    geojson_geom = json.dumps(_geom)
    geojson_geom = geojson.loads(geojson_geom)
    geojson_str = str(center_median(geojson_geom, n_iter))
    
    return wkt_from_geojson(geojson_str)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_CENTERMEDIAN
(GEOMETRY)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__CENTERMEDIAN(ST_ASGEOJSON($1)::VARCHAR(MAX), 100))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__CENTROID
(geom VARCHAR(MAX))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.transformations import centroid, PRECISION, wkt_from_geojson
    import geojson
    import json
    
    if geom is None:
        return None

    _geom = json.loads(geom)
    _geom['precision'] = PRECISION
    geojson_geom = json.dumps(_geom)
    geojson_geom = geojson.loads(geojson_geom)
    geojson_str = str(centroid(geojson_geom))
    
    return wkt_from_geojson(geojson_str)

$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_CENTROID
(GEOMETRY)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__CENTROID(ST_ASGEOJSON($1)::VARCHAR(MAX)))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_CENTEROFMASS
(GEOMETRY)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT carto.ST_CENTROID($1)
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__DESTINATION
(geom VARCHAR(MAX), distance FLOAT8, bearing FLOAT8, units VARCHAR(15))
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.transformations import destination, PRECISION, wkt_from_geojson
    import geojson
    import json

    if geom is None or distance is None or bearing is None or units is None:
        return None

    _geom = json.loads(geom)
    _geom['precision'] = PRECISION
    geojson_geom = json.dumps(_geom)
    geojson_geom = geojson.loads(geojson_geom)
    geojson_str = str(destination(geojson_geom, distance, bearing, units))
    
    return wkt_from_geojson(geojson_str)

$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION carto.ST_DESTINATION
(GEOMETRY, FLOAT8, FLOAT8)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__DESTINATION(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3, 'kilometers'))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.ST_DESTINATION
(GEOMETRY, FLOAT8, FLOAT8, VARCHAR(15))
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__DESTINATION(ST_ASGEOJSON($1)::VARCHAR(MAX), $2, $3, $4))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION carto.__GREATCIRCLE
(start_point VARCHAR(MAX), end_point VARCHAR(MAX), n_points INT)
RETURNS VARCHAR(MAX)
STABLE
AS $$
    from carto_analytics_toolbox_core.transformations import great_circle, PRECISION, wkt_from_geojson
    import geojson
    import json

    if start_point is None or end_point is None or n_points is None:
        return None

    _geom = json.loads(start_point)
    _geom['precision'] = PRECISION
    start_geom = json.dumps(_geom)
    start_geom = geojson.loads(start_geom)

    _geom = json.loads(end_point)
    _geom['precision'] = PRECISION
    end_geom = json.dumps(_geom)
    end_geom = geojson.loads(end_geom)
    geojson_str = str(great_circle(start_geom, end_geom, n_points))

    return wkt_from_geojson(geojson_str)

$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION carto.ST_GREATCIRCLE
(GEOMETRY, GEOMETRY)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__GREATCIRCLE(ST_ASGEOJSON($1)::VARCHAR(MAX), ST_ASGEOJSON($2)::VARCHAR(MAX), 100))
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.ST_GREATCIRCLE
(GEOMETRY, GEOMETRY, INT)
RETURNS GEOMETRY
STABLE
AS $$
    SELECT ST_GEOMFROMTEXT(carto.__GREATCIRCLE(ST_ASGEOJSON($1)::VARCHAR(MAX), ST_ASGEOJSON($2)::VARCHAR(MAX), $3))
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION carto.VERSION_CORE
()
RETURNS VARCHAR
IMMUTABLE
AS $$
    SELECT '2022.10.07'
$$ LANGUAGE sql;
