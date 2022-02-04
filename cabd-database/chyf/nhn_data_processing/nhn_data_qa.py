import psycopg2 as pg2
import sys

def log(message):
    if (0):
        print(message);

def run_qa(conn, workunit):
    workingSchema = "nhn" + workunit.lower()  
    print("Running QA: " + workunit)       
     
    tables = f"""
        create table if not exists {workingSchema}.qaerrors(
          id serial primary key,
          type varchar,
          message varchar,
          geometry geometry(POINT)
        );
        
        truncate {workingSchema}.qaerrors;
    """
    log (tables)
    with conn.cursor() as cursor:
        cursor.execute(tables)   
        
    #FLOW DIRECTION POPULATED
    qa1 = f"""
    --flow direction not set for terminal node
    insert into {workingSchema}.qaerrors (type, message, geometry)
    select 'ERROR', 'terminal node flow direction not set ' || a.id, a.geometry  
    FROM {workingSchema}.terminal_node a where flow_direction is null;
    """
    log (qa1)
    with conn.cursor() as cursor:
        cursor.execute(qa1)       

    #NULL OR EMPTY GEOMETRIES
    qa1 = f"""
        --empty or null geometries
        select a.cnt + b.cnt + c.cnt 
        from 
        (select count(*) as cnt from {workingSchema}.eflowpath where geometry is null or st_isempty(geometry)) a,
        (select count(*) as cnt from {workingSchema}.ecatchment where geometry is null or st_isempty(geometry)) b, 
        (select count(*) as cnt from {workingSchema}.aoi where geometry is null or st_isempty(geometry) ) c,
        (select count(*) as cnt from {workingSchema}.shoreline where geometry is null or st_isempty(geometry) ) d
    """
    log (qa1)
    with conn.cursor() as cursor:
        cursor.execute(qa1)
        cnt = cursor.fetchone()[0]
        if (cnt != 0):
            qa1 = f"INSERT INTO {workingSchema}.qaerrors(type, message) values ('ERROR', 'There are one or more empty or null geometries');";
            cursor.execute(qa1)
            

    #SIMPLE/VALIDATE GEOMETRIES
    qa2 = f"""
       insert into {workingSchema}.qaerrors (type, message, geometry)
       select 'ERROR', 'Geometry is not valid or simple ' || id, st_pointonsurface(geometry)
       FROM 
       (
        select id, 'eflowpath', geometry from {workingSchema}.eflowpath where not st_isvalid(geometry) or not st_issimple(geometry)
        union
        select id, 'ecatchment', geometry from {workingSchema}.ecatchment where not st_isvalid(geometry) or not st_issimple(geometry)
        union
        select id, 'shoreline', geometry from {workingSchema}.shoreline where not st_isvalid(geometry) or not st_issimple(geometry)
        union
        select id, 'aoi', geometry from {workingSchema}.aoi where not st_isvalid(geometry) or not st_issimple(geometry)
      ) foo;
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)

    #WATERBODY WITHIN WATERBODY
    qa2 = f"""
        --interior of polygon exactly matches exterior of another polygon
        insert into {workingSchema}.qaerrors (type, message, geometry)
        SELECT 'ERROR', 'waterbody matches an island inside another waterbody: ' || ex.id || ' ' || it.id, ex.pnt
        from
          (select id, st_exteriorring(geometry) as geometry, st_pointonsurface(geometry) as pnt from {workingSchema}.ecatchment) as ex,
          (select id, st_interiorringn(geometry, generate_series(1,  st_numinteriorrings(geometry))) as geometry
          from {workingSchema}.ecatchment where st_numinteriorring(geometry) > 0) as it
        where ex.id != it.id and ex.geometry && it.geometry and st_equals(ex.geometry, it.geometry)
        
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)

    #WATERBODIES OVERLAP
    qa2 = f"""
        --interior of polygon exactly matches exterior of another polygon
        insert into {workingSchema}.qaerrors (type, message, geometry)
        select 'ERROR', 'waterbodies overlap ' || id1 || ' ' || id2, pnt
        FROM
        (
          select a.id as id1, b.id as id2, st_pointonsurface(a.geometry) as pnt
          from {workingSchema}.ecatchment a, {workingSchema}.ecatchment b
          WHERE a.id != b.id and a.geometry && b.geometry and st_relate(a.geometry, b.geometry, 'T********')
        ) foo
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)            


    #FLOWPATHS OVERLAP (interior intersects)
    qa2 = f"""
        insert into {workingSchema}.qaerrors (type, message, geometry)
        select 'ERROR', 'flowpaths overlap (interior intersection)' || a.id || ' ' || b.id, st_intersection(a.geometry, b.geometry) as pnt
        from {workingSchema}.eflowpath a, {workingSchema}.eflowpath b
        WHERE a.id != b.id and a.geometry && b.geometry and st_relate(a.geometry, b.geometry, 'T********')
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)

    #FLOWPATHS NEAR (but not noded)
    qa2 = f"""
        insert into {workingSchema}.qaerrors (type, message, geometry)
        select 'WARNING', 'flowpaths near but not noded ' || a.id || ' ' || b.id, st_closestpoint(a.geometry,b.geometry) as pnt
        from {workingSchema}.eflowpath a, {workingSchema}.eflowpath b
        where  a.id != b.id and 
        st_dwithin(a.geometry, b.geometry, 0.000001) and 
        st_disjoint(a.geometry, b.geometry) 
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)
            
    #NON_SEKELETON FLOWPATH IN WATERBODY
    qa2 = f"""
        insert into {workingSchema}.qaerrors (type, message, geometry)
        select 'ERROR', 'flowpath inside waterbody' || a.id || ' ' || b.id, st_pointonsurface(st_intersection(a.geometry, b.geometry)) as pnt
        from {workingSchema}.eflowpath a, {workingSchema}.ecatchment b
        WHERE a.ef_type != 3 and a.id != b.id and a.geometry && b.geometry and st_relate(a.geometry, b.geometry, 'T********')
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)            

    #DEGREE 4+ nodes (for review)
    qa2 = f"""
        with nodes as (
          select st_startpoint(geometry) as geometry from {workingSchema}.eflowpath
          union all
          select st_endpoint(geometry) as geometry from {workingSchema}.eflowpath
         )
        insert into {workingSchema}.qaerrors (type, message, geometry)
         select 'WARNING', 'degree 4 node', geometry
         from nodes
         group by geometry having count(*) >= 4;
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)

    #waterdefinition = -1 
    qa2 = f"""
        insert into {workingSchema}.qaerrors (type, message, geometry)
         select 'WARNING', 'WARNING: catchment with unknown subtype (ec_subtype = 99) and name: ' || id || ' ' || lakename1 || ' ' || lakename2 || ' ' || rivername1 || ' ' || rivername2, st_pointonsurface(geometry)
        FROM 
        {workingSchema}.ecatchment 
        WHERE 
        ec_subtype = 99 
        and 
        ((lakename1 is not null and lakename1 != '') or
        (lakename2 is not null and lakename2 != '') or
        (rivername1 is not null and rivername1 != '') or
        (rivername2 is not null and rivername2 != ''));
         
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)

    #touching waterbodies with different permanency attribute 
    #qa2 = f"""
    #    insert into {workingSchema}.qaerrors (type, message, geometry)
    #     select distinct 'WARNING', 'WARNING: waterbodies with same subtype touch with different permanency value', st_pointonsurface(st_intersection(a.geometry, b.geometry)) 
    #    FROM 
    #    {workingSchema}.ecatchment a,
    #    {workingSchema}.ecatchment b
    #    WHERE 
    #    a.ec_type = b.ec_type and a.ec_subtype = b.ec_subtype
    #    and a.permanency != b.permanency 
    #    and a.geometry && b.geometry
    #    and st_intersects(a.geometry, b.geometry) ;                 
    #"""
    #log (qa2)
    #with conn.cursor() as cursor:
    #    cursor.execute(qa2)
        
        
    #touching waterbodies with more than 3 shared vertices 
    qa2 = f"""
        insert into {workingSchema}.qaerrors (type, message, geometry)
         select distinct 'WARNING', 'WARNING: waterbodies with same subtype touch and share more than 3 vertices', st_pointonsurface(st_intersection(a.geometry, b.geometry)) 
        FROM 
        {workingSchema}.ecatchment a,
        {workingSchema}.ecatchment b
        WHERE 
        a.ec_type = b.ec_type and a.ec_subtype = b.ec_subtype
        and a.permanency != b.permanency 
        and a.geometry && b.geometry
        and st_intersects(a.geometry, b.geometry)
        and st_npoints(st_intersection(a.geometry, b.geometry)) > 3;
    """
    log (qa2)
    with conn.cursor() as cursor:
        cursor.execute(qa2)

    qa1 = f'SELECT type, message, st_astext(geometry) from {workingSchema}.qaerrors';
    with conn.cursor() as cursor:
        cursor.execute(qa1)
        rows = cursor.fetchall()
        for row in rows:
            print(f"{row[0]},{row[1]},{row[2]}");
               
    conn.commit()
    
    print ("QA Complete: " + workunit)


if __name__ == '__main__':
    if len(sys.argv) != 7:
        print("Invalid Usage: nhn_data_qa.py <host> <port> <dbname> <dbuser> <dbpassword> <nhnworkunit>")
    
    dbHost = sys.argv[1]
    dbPort = sys.argv[2]
    dbName = sys.argv[3]
    dbUser = sys.argv[4]
    dbPassword = sys.argv[5]

    nhnworkunit = sys.argv[6].upper()
          
    conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)
    run_qa(conn, nhnworkunit);
