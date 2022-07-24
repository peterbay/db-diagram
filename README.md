# db-diagram

Database diagram generator. Simple and configurable with xml output for [Drawio](https://app.diagrams.net/)

## How to use

    Usage: python3 db-diagram.py [options]

    Options
    -e, --extract                   Extract descriptions from DB into config file
    -c, --config <value>            Config file
    -o, --output                    Output file
    -v, --verbose                   Add more info to output

## Examples of usage

Get columns description from database and save into config file.
```
python3 db-diagram.py -c conf/demo.yaml -e
```

Generate output file from obtained informations.
```
python3 db-diagram.py -c conf/demo.yaml -o sample.xml
```

## Config file structure

```
database:
  dsn: postgres://user:password@host:5432/dbname
  type: postgresql

structure:
  schema: schema_name
  table: .*

diagram:
  addColumnComment: true
  appendColumnType: true
  changePlacementDirection: true
  childOffsetFix: true
  childPriorityFix: true
  groupTableWithComment: true
  columnCenter: 1000
  columnMaxHeight: 1600
  commentWidth: 200
  constraintColor: 999999
  constraintOpacity: 90
  constraintWidth: 2
  foreignKeyColor: FF99FF
  layers: true
  parentPriorityFix: true
  prependSchemaName: true
  primaryKeyColor: FFCC99
  tableWidth: 200
  uniqueColor: E6FFCC

descriptions:
  # place for descriptions
  schema_name.table_name.column_name: description for column

```

### Database parameters

|key|description|
|---|---|
|dsn|Connection string for database|
|type|Database type \[postgresql\]|

### Structure parameters

|key|description|
|---|---|
|schema|Regular expression for filtering schema names|
|table|Regular expression for filtering table names|

### Description parameters
|key|description|
|---|---|
|schema.table.column|Description for selected column from schema/table|

### Diagram parameters

|key|default value|description|
|---|---|---|
|addColumnComment|false|Generate diagram with columns comment|
|appendColumnType|false|Append column type after column name (column_name \[text\])|
|changePlacementDirection|true|Change direction for placing tables to diagram|
|childOffsetFix|false|Fix offset for parent/child placement|
|childPriorityFix|false|Sort child tables by their priority|
|groupTableWithComment|false|Group table with comments into group|
|columnCenter|1000|Placement from top for first table|
|columnMaxHeight|1000||
|commentWidth|200|Column comment width|
|constraintColor|999999|Color for constraint line (hexadecimal)|
|constraintOpacity|100|Constraint line opacity (0 to 100)|
|constraintWidth|1|Constraint line width|
|foreignKeyColor|FF99FF|Color for FOREIGN KEY column (hexadecimal)|
|layers|false|Generate schemas and foreign connections into separated layers|
|parentPriorityFix|false|Sort parent tables by their priority|
|prependSchemaName|false|Prepend schema name before table name|
|primaryKeyColor|FFCC99|Color for PRIMARY KEY column (hexadecimal)|
|tableWidth|200|Table width|
|uniqueColor|E6FFCC|Color for UNIQUE KEY column (hexadecimal)|

## Requirements
Python3 and python libraries

```
sudo pip3 install psycopg2
# or
sudo pip3 install psycopg2-binary

sudo pip3 install PyYAML
```