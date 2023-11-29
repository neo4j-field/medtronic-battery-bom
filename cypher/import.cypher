//creates indexes in neo4j
CREATE INDEX FOR (a:Activity) ON (a.id);
CREATE INDEX FOR (a:Activity) ON (a.time); // two distinct indexes for :Activity, instead of a composite index , because I do not expect both properties to be in a predicate at once.
CREATE INDEX FOR (l:Lot) ON (l.id);
CREATE INDEX FOR (p:Product) ON (p.id);
CREATE INDEX FOR (l:Lot) ON (l.id);
CREATE INDEX FOR (s:Step) ON (s.id);
CREATE INDEX FOR (o:Operator) ON (o.id);
CREATE INDEX FOR (c:Component) ON (c.id);
CREATE INDEX FOR (l:LotLocation) ON (l.id);


//Activity Nodes
//question: is SysId a unique identifier for each activity
:auto
WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.SYSID as activityId, row.NAME as activityName, row.TXNTIME as txTime
CALL {
    WITH activityName, activityId, txTime
    WITH
        substring(txTime, 0, 4) + '-' +  // Year
        substring(txTime, 4, 2) + '-' +  // Month
        substring(txTime, 6, 2) + 'T' +  // Day
        substring(txTime, 9, 2) + ':' +  // Hour
        substring(txTime, 11, 2) + ':' + // Minute
        substring(txTime, 13, 2) + '.' +
        substring(txTime, 15, 3)
        AS formattedDateTime, activityName, activityId
    MERGE (a:Activity {id: activityId})
    ON CREATE
        SET a.name = activityName,
            a.time = datetime(formattedDateTime)
} IN TRANSACTIONS OF 10000 ROWS;




//LotLocation Nodes
//should there be any other properties on this?
:auto WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.LOTLOCATION as lotlocation
CALL {
  WITH lotlocation
  MERGE (l:LotLocation {id: lotlocation})
} IN TRANSACTIONS OF 10000 rows;




//Lot/BatteryLot Nodes
:auto WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.LOTID as lotId
CALL {
  WITH lotId
  MERGE (l:Lot {id: lotId})
} IN TRANSACTIONS OF 10000 rows;



//Step Nodes
//There are only 10 distinct STEPIDs but there are like 50~ LOTTIMESTAMP
:auto WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.STEPID as stepId
CALL {
  WITH stepId
  MERGE (s:Step {id: stepId})
} IN TRANSACTIONS OF 10000 rows;


//Operator Nodes
:auto WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.USERID as userId
CALL {
  WITH userId
  MERGE (o:Operator {id: userId})
} IN TRANSACTIONS OF 10000 rows;


// Activity :NEXT chain/sequence
// grouping keys are on step and lot. So sequences will be grouped by those two fields/dimensions
WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.LOTID as lotId, row.STEPID as stepId, row.SYSID as activityId
MATCH (step:Step {id: stepId})
MATCH (lot:Lot {id: lotId})
MATCH (activity:Activity {id: activityId})
WITH lot,step, collect(activity) as activities
UNWIND range(0, size(activities) - 2) as i
WITH activities[i] as currentActivity, activities[i + 1] as nextActivity
MERGE (currentActivity) - [:NEXT] -> (nextActivity);


//Step to (Battery) Lot
WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.STEPID as stepId, row.LOTID as lotId
MATCH (step:Step {id: stepId})
MATCH (lot:Lot {id: lotId})
MERGE (step) - [:PART_OF] -> (lot);


// Activities to Step
WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.SYSID as activityId, row.STEPID as stepId
MATCH (step:Step {id: stepId})
MATCH (activity:Activity {id: activityId})
MERGE (activity) - [:OCCURS_ON] -> (step);

//Activities to Operator
WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.USERID as userId, row.SYSID as activityId
MATCH (activity:Activity {id: activityId})
MATCH (operator:Operator {id: userId})
MERGE (activity) - [:PERFORMED_BY] -> (operator);

//Activities to LotLocation
WITH 'file:///lot_info.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.LOTLOCATION as lotId, row.SYSID as activityId
MATCH (lot:LotLocation {id: lotId})
MATCH (activity:Activity {id: activityId})
MERGE (activity) - [:PERFORMED_AT] -> (lot);


//Component Nodes (Raw Battery)
//Component Nodes (Raw Battery)
:auto WITH 'file:///lot_subassembly.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row as filtered_row
WHERE row.LOTID STARTS WITH 'E'
MERGE (c:Component:RawBattery {id: filtered_row.LOTID});

//Component Nodes (Battery Connector)
:auto WITH 'file:///lot_subassembly.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row as filtered_row
WHERE row.LOTID =~ '^[0-9].*'
MERGE (c:Component:BatteryConnector {id: filtered_row.LOTID});


//Component Nodes to Lot Nodes
:auto WITH 'file:///lot_subassembly.csv' as file
LOAD CSV WITH HEADERS FROM file as row
WITH row.LOTID as batteryId, row.TOPLEVELLOTID as assemblyId
MATCH (component:Component {id: batteryId})
MATCH (lot:Lot {id: assemblyId})
MERGE (lot) - [:HAS_CHILD] -> (component);



//READ QUERIES
//Activity Sequences ... query just shows the different linked lists/sequences of activities we generated (not filtered on anything else).
MATCH (startActivity:Activity)
WHERE NOT ()-[:NEXT]->(startActivity)
MATCH (startActivity)-[:OCCURS_ON]->(step:Step)-[:PART_OF]->(lot:Lot)
WITH startActivity, step, lot
MATCH path = (startActivity)-[:NEXT*]->(endActivity:Activity)
WHERE NOT (endActivity)-[:NEXT]->()
WITH lot, step, path
UNWIND nodes(path) AS activity
WITH lot, step, COLLECT(DISTINCT activity) AS activities
WITH lot, COUNT(DISTINCT activities) AS totalActivities
RETURN lot AS Lot, totalActivities AS TotalActivities
ORDER BY TotalActivities DESC
LIMIT 10



// Activities and steps associated with a specific operator
MATCH (o:Operator {id: 'johnm4'})
WITH o
MATCH (o) <- [:PERFORMED_BY] - (startActivity:Activity)
WHERE NOT EXISTS ((startActivity) <- [:NEXT] - (:Activity))
WITH o, startActivity
MATCH activityPath = (o)<-[:PERFORMED_BY]-(startActivity)
((:Activity)-[:NEXT]->(a_i:Activity)){1,25}
(a:Activity WHERE NOT EXISTS {(a)-[:NEXT]->()})
WITH activityPath, startActivity, a_i
UNWIND [startActivity]+a_i AS act
MATCH occurs_path = (act)-[o:OCCURS_ON]->(s:Step)
WITH activityPath, occurs_path
RETURN occurs_path

//Activities and steps associated with a specific operator -- with date filter.

// Activities and steps associated with a specific operator
MATCH (o:Operator {id: 'johnm4'})
WITH o
MATCH (o) <- [:PERFORMED_BY] - (startActivity:Activity)
WHERE NOT EXISTS ((startActivity) <- [:NEXT] - (:Activity)) AND
datetime(startActivity.time) >= datetime('2023-08-31T00:00:00') AND
datetime(startActivity.time) <= datetime('2023-12-31T23:59:59')
WITH o, startActivity
MATCH activityPath = (o)<-[:PERFORMED_BY]-(startActivity)
((:Activity)-[:NEXT]->(a_i:Activity)){1,25}
(a:Activity WHERE NOT EXISTS {(a)-[:NEXT]->()})
WITH activityPath, startActivity, a_i
UNWIND [startActivity]+a_i AS act
MATCH occurs_path = (act)-[o:OCCURS_ON]->(s:Step)
WITH activityPath, occurs_path
RETURN occurs_path


// Activities and steps associated with a specific operator
MATCH (o:Operator {id: 'johnm4'})
WITH o
MATCH (o) <- [:PERFORMED_BY] - (startActivity:Activity)
WHERE NOT EXISTS ((startActivity) <- [:NEXT] - (:Activity))
WITH o, startActivity
MATCH activityPath = (o)<-[:PERFORMED_BY]-(startActivity)
((:Activity)-[:NEXT]->(a_i:Activity)){1,25}
(a:Activity WHERE NOT EXISTS {(a)-[:NEXT]->()})
WITH activityPath, startActivity, a_i
UNWIND [startActivity]+a_i AS act
MATCH occurs_path = (act)-[o:OCCURS_ON]->(s:Step)
WITH activityPath, occurs_path
RETURN occurs_path



//first activities
MATCH (startActivity:Activity)
WHERE NOT ()-[:NEXT]->(startActivity)
WITH startActivity
MATCH path = (startActivity)-[:NEXT*]->(endActivity:Activity)
WHERE NOT (endActivity)-[:NEXT]->()
RETURN path, length(path) as activityCount
LIMIT 1



//First Activity by an Operator
MATCH (o:Operator {id: 'johnm4'})
WITH o
MATCH (o)-[:PERFORMED_BY]->(activity:Activity)
WHERE NOT EXISTS((activity)<-[:NEXT]-(:Activity))
RETURN act



WITH '20230713 103637000' as line
WITH
  substring(line, 0, 4) + '-' +
  substring(line, 4, 2) + '-' +
  substring(line, 6, 5) + ':' +
  substring(line, 11, 2) + ':' +
  substring(line, 13, 2) + '.' +
  substring(line, 15) AS formattedDateTime
RETURN datetime(formattedDateTime)


Text cannot be parsed to a DateTime
"2023-07-13 10:36:37.000"


