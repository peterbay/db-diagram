
import re
import sys
import argparse
import yaml
import traceback

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
except:
    pass

class dbLayer:
    _dsn = None
    _config = None
    _db_structure = None

    def __init__(self, dsn, config):
        self._dsn = dsn
        self._config = ExtConfig(config)

        self.connect()
        self._db_structure = DBStructure(self._config)

    def add_branch(self, tree, vector, value):
        key = vector[0]

        if len(vector) == 1:
            tree[key] = value
        else:
            self.add_branch(tree[key] if key in tree else {}, vector[1:], value)

    def get_table_structure(self):
        pass

    def get_constraints(self):
        pass

    def to_diagram(self):
        return self._db_structure.to_diagram()

    def get_descriptions(self):
        return self._db_structure.get_descriptions()


class pgLayer(dbLayer):
    _connection = None
    _cursor = None

    def connect(self):
        try:
            self._connection = psycopg2.connect(self._dsn)
            self._connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self._cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            print(e)

    def get_table_structure(self):
        query_tables = """
            SELECT
                col.table_schema::TEXT,
                col.table_name::TEXT,
                col.column_name::TEXT,
                col.udt_name::TEXT,
                col.ordinal_position,
                pgd.description::TEXT
            FROM information_schema.columns col
            LEFT JOIN pg_catalog.pg_statio_all_tables as st ON (col.table_schema=st.schemaName and col.table_name=st.relname)
            LEFT JOIN pg_catalog.pg_description pgd ON (pgd.objsubid = col.ordinal_position and pgd.objoid=st.relid)
            ORDER by col.table_schema::TEXT, col.table_name::TEXT, col.ordinal_position
        """

        self._cursor.execute(query_tables)
        data = self._cursor.fetchall()
        for entry in data:
            self._db_structure.add_table_entry(entry)

    def get_constraints(self):
        query_constraint = """
            SELECT
                tc.constraint_type::TEXT,
                tc.table_schema::TEXT, 
                tc.constraint_name::TEXT, 
                tc.table_name::TEXT, 
                kcu.column_name::TEXT, 
                ccu.table_schema::TEXT AS foreign_table_schema,
                ccu.table_name::TEXT AS foreign_table_name,
                ccu.column_name::TEXT AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema;
        """

        self._cursor.execute(query_constraint)
        data = self._cursor.fetchall()
        for x in data:
            self._db_structure.add_constraint(x)


class ExtConfig:
    def __init__(self, config):
        self.config = config

    def get(self, section, key, default):
        try:
            return self.config[section][key]
        except Exception:
            return default

    def items(self, section):
        try:
            if section:
                return self.config[section]
            return self.config
        except Exception:
            return {}


class DiagramLayer:
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __str__(self):
        return f"""<mxCell id="{self.id}" value="{self.name}" parent="0"/>"""


class DiagramHeaderCell:
    def __init__(self, config, parent_id, id, value, rows, x, y):
        self.config = config
        self.id = id
        self.parent_id = parent_id
        self.value = value
        self.rows = rows
        self.x = x
        self.y = y
        self.height = (self.rows + 1) * 26
        self.width = int(self.config.get('diagram', 'tableWidth', 200))

    def __str__(self):
        style = "swimlane;fontStyle=1;childLayout=stackLayout;horizontal=1;startSize=26;horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=1;marginBottom=0;labelBackgroundColor=none;align=center;gradientDirection=north;swimlaneFillColor=none;gradientColor=#E6E6E6;rounded=0;swimlaneLine=1;direction=east;strokeColor=#4D4D4D;"

        return '\n'.join([
            f"""<mxCell id="{self.id}" value="{self.value}" style="{style}" vertex="1" parent="{self.parent_id}">""",
            f"""<mxGeometry x="{self.x}" y="{self.y}" width="{self.width}" height="{self.height}" as="geometry"/>""",
            f"""</mxCell>"""
        ])


class DiagramCell:
    def __init__(self, config, parent_id, id, value, parent, row_number, color):
        self.config = config
        self.id = id
        self.parent_id = parent_id
        self.value = value
        self.parent = parent
        self.width = int(self.config.get('diagram', 'tableWidth', 200))
        self.color = f'fillColor=#{color};' if color else ''
        self.y = (row_number + 1) * 26

    def __str__(self):
        style = f"text;align=left;verticalAlign=top;spacingLeft=4;spacingRight=4;overflow=hidden;rotatable=0;points=[[0,0.5],[1,0.5]];portConstraint=eastwest;labelBackgroundColor=none;{self.color}"

        return '\n'.join([
            f"""<mxCell id="{self.id}" value="{self.value}" style="{style}" vertex="1" parent="{self.parent}">""",
            f"""<mxGeometry y="{self.y}" width="{self.width}" height="26" as="geometry"/>""",
            f"""</mxCell>"""
        ])


class DiagramConstraint:
    def __init__(self, config, parent_id, id, source, target, entry_x, exit_x):
        self.config = config
        self.id = id
        self.parent_id = parent_id
        self.source = source
        self.target = target
        self.entry_x = entry_x
        self.exit_x = exit_x
        self.color = str(self.config.get('diagram', 'constraintColor', '999999'))
        self.width = int(self.config.get('diagram', 'constraintWidth', 1))
        self.opacity = int(self.config.get('diagram', 'constraintOpacity', 100))

    def __str__(self):
        style = f"edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exit_x={self.exit_x};exitY=0.5;exitDx=0;exitDy=0;entry_x={self.entry_x};entryY=0.5;entryDx=0;entryDy=0;curved=1;strokeColor=#{self.color};strokeWidth={self.width};opacity={self.opacity};"

        return '\n'.join([
            f"""<mxCell id="{self.id}" source="{self.source}" target="{self.target}" parent="{self.parent_id}" style="{style}" edge="1">""",
            f"""<mxGeometry relative="1" as="geometry"/>""",
            f"""</mxCell>"""
        ])


class DiagramComment:
    def __init__(self, config, parent_id, column_id, table_x, table_y, row_number, description):
        self.config = config
        self.parent_id = parent_id
        self.column_id = column_id
        self.commentId = column_id + 1
        self.joinId = column_id + 2
        self.description = description if description else ''
        self.width = int(self.config.get('diagram', 'commentWidth', 140))
        tableWidth = int(self.config.get('diagram', 'tableWidth', 200))
        self.x = table_x + tableWidth + 40
        self.y = table_y + (row_number + 1) * 26 + 4

    def __str__(self):
        style = f"rounded=0;whiteSpace=wrap;html=1;align=left;spacingTop=0;spacingLeft=4;fillColor=#f5f5f5;fontColor=#333333;strokeColor=#B3B3B3;"
        style_joining = f"edgeStyle=entityRelationEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exit_x=0;exitY=0.5;exitDx=0;exitDy=0;fillColor=#f5f5f5;strokeColor=#B3B3B3;jumpStyle=none;elbow=vertical;entryX=1;entryY=0.5;entryDx=0;entryDy=0;exitX=0;"

        return '\n'.join([
            f"""<mxCell id="{self.commentId}" value="{self.description}" style="{style}" vertex="1" parent="{self.parent_id}">""",
            f"""<mxGeometry x="{self.x}" y="{self.y}" width="{self.width}" height="18" as="geometry"/>""",
            f"""</mxCell>"""
            f"""<mxCell id="{self.joinId}" source="{self.commentId}" target="{self.column_id}" parent="{self.parent_id}" style="{style_joining}" edge="1">""",
            f"""<mxGeometry relative="1" as="geometry"/>"""
            f"""</mxCell>"""
        ])

class DiagramGroup:
    def __init__(self, config, parent_id, group_id, x, y, width, height):
        self.config = config
        self.parent_id = parent_id
        self.group_id = group_id
        self.x = x
        self.y = y
        self.width = width
        self.height = height

    def __str__(self):
        return '\n'.join([
            f"""<mxCell id="{self.group_id}" value="" style="group" vertex="1" connectable="0" parent="{self.parent_id}">""",
            f"""<mxGeometry x="{self.x}" y="{self.y}" width="{self.width}" height="{self.height}" as="geometry" />""",
            f"""</mxCell>"""
        ])


class DBTable:
    def __init__(self, config, parent_id, id, group_id, schema, name, show_comments):
        self.config = config
        self.parent_id = parent_id
        self.group_id = group_id
        self.id = id
        self.schema = schema
        self.name = name
        self.show_comments = show_comments
        self.x = 0
        self.y = 0
        self.columns = []
        self.column_number = 0
        self.prepend_schema = bool(self.config.get('diagram', 'prependSchemaName', False))
        self.priority = None
        self.priority_up = 0
        self.priority_down = 0
        self.childs = []
        self.parents = []
        self.used_in_diagram = False

    def get_schema(self):
        return self.schema

    def get_name(self):
        return self.name

    def set_priority(self, add):
        if not self.priority:
            self.priority = 1000

        add_value = 0
        if add > 0:
            self.priority_up =  self.priority_up + 1
            add_value = self.priority_up * 2

        else:
            self.priority_down = self.priority_down + 1
            add_value = -10 + self.priority_down * -2
    
        self.priority = self.priority + add_value

    def get_priority(self):
        if not self.priority:
            return 10000
        return self.priority

    def set_column(self, column):
        self.column_number = column

    def get_column(self):
        return self.column_number

    def set_position(self, x, y):
        self.x = x
        self.y = y

    def add_child(self, child_table):
        self.childs.append(child_table)

    def add_parent(self, parent_table):
        self.parents.append(parent_table)

    def get_childs(self):
        return self.childs

    def get_parents(self):
        return self.parents

    def set_used_in_diagram(self):
        self.used_in_diagram = True

    def get_used_in_diagram(self):
        return self.used_in_diagram

    def get_column_counts(self):
        return len(self.columns)

    def add_column(self, id, name, udt_name, position, description):
        self.columns.append({
            'id': id,
            'name': name,
            'udt_name': udt_name,
            'position': position,
            'description': description,
            'types': []
        })

    def set_column_type(self, name, type):
        for column in self.columns:
            if column['name'] == name:
                column['types'].append(type)
                return 

    def __str__(self):
        if not len(self.columns):
            return ''

        diagram = []
        table_name = self.name
        if self.prepend_schema:
            table_name = f'{self.schema}.{self.name}'

        parent_id = self.parent_id
        x = self.x
        y = self.y

        group_cells = bool(self.config.get('diagram', 'groupTableWithComment', False))

        if group_cells:
            width = int(self.config.get('diagram', 'tableWidth', 200))
            height = (len(self.columns) + 1) * 26

            if self.show_comments:
                width = width + int(self.config.get('diagram', 'commentWidth', 140)) + 40

            diagram.append(str(DiagramGroup(self.config, parent_id, self.group_id, x, y, width, height)))
            parent_id = self.group_id
            x = 0
            y = 0

        diagram.append(str(DiagramHeaderCell(self.config, parent_id, self.id, table_name, len(self.columns), x, y)))

        append_column_type = bool(self.config.get('diagram', 'appendColumnType', False))

        row_number = 0
        for column in self.columns:
            column_name = column['name']
            if append_column_type:
                udt_name = column['udt_name']
                column_name = f'{column_name} [{udt_name}]'

            color = None

            if 'FOREIGN KEY' in column['types']:
                column_name = f'[FK] {column_name}'
                color = str(self.config.get('diagram', 'foreignKeyColor', 'FF99FF'))

            if 'UNIQUE' in column['types']:
                column_name = f'[U] {column_name}'
                color = str(self.config.get('diagram', 'uniqueColor', 'E6FFCC'))

            if 'PRIMARY KEY' in column['types']:
                column_name = f'[PK] {column_name}'
                color = str(self.config.get('diagram', 'primaryKeyColor', 'FFCC99'))

            diagram.append(str(DiagramCell(self.config, parent_id, column['id'], column_name, self.id, row_number, color)))

            if self.show_comments:
                diagram.append(str(DiagramComment(self.config, parent_id, column['id'], x, y, row_number, column['description'])))

            row_number = row_number + 1

        return ('\n'.join(diagram))

class DBStructure:
    def __init__(self, config):
        self.config = config
        self.tables = []
        self.layers = {}
        self.id = 2
        self.x = 100
        self.y = 100
        self.structure = {}
        self.structure_ids = {}
        self.connections = []
        self.use_layers = bool(config.get('diagram', 'layers', False))
        self.add_comment = bool(config.get('diagram', 'addColumnComment', False))
        self.column_center = int(config.get('diagram', 'columnCenter', 1000))
        self.column_max_height = int(config.get('diagram', 'columnMaxHeight', 1000))
        self.child_offset_fix = bool(config.get('diagram', 'childOffsetFix', False))
        self.child_priority_fix = bool(config.get('diagram', 'childPriorityFix', False))
        self.parent_priority_fix = bool(config.get('diagram', 'parentPriorityFix', False))
        self.change_placement_direction = bool(config.get('diagram', 'changePlacementDirection', True))
        self.schema_filter = [x.strip() for x in str(self.config.get('structure', 'schema', '')).split(',')]
        self.table_filter = [re.compile(x.strip()) for x in str(self.config.get('structure', 'table', '')).split(',')]
        if not self.use_layers:
            self.layers['main'] = self.get_next_id()

        self.descriptions = {}
        description_items = self.config.items('descriptions')
        for path, description in description_items.items():
            self.descriptions[path] = description

    def get_next_id(self, increment = 1):
        id = self.id + 1
        self.id = self.id + increment
        return id

    def get_table(self, schema, name):
        for table in self.tables:
            if table.get_name() == name and table.get_schema() == schema:
                return table

        if self.use_layers:
            if not schema in self.layers:
                self.layers[schema] = self.get_next_id()

            parent_id = self.layers[schema]

        else:
            parent_id = self.layers['main']

        table = DBTable(self.config, parent_id, self.get_next_id(), self.get_next_id(), schema, name, self.add_comment)
        self.tables.append(table)

        return table

    def add_structure_entry(self, schema, table, column, id, description):
        if not schema in self.structure:
            self.structure[schema] = {}

        if not table in self.structure[schema]:
            self.structure[schema][table] = {}

        self.structure[schema][table][column] = id
        self.structure_ids[id] = {
            'schema': schema,
            'table': table,
            'column': column,
            'description': description
        }

    def get_structure_entry(self, schema, table, column):
        if not schema in self.structure:
            return None

        if not table in self.structure[schema]:
            return None

        if not column in self.structure[schema][table]:
            return None
        
        return self.structure[schema][table][column]

    def check_table_filter(self, table_name):
        if not len(self.table_filter):
            return True

        for tf in self.table_filter:
            if tf.match(table_name):
                return True

        return False

    def get_descriptions(self):
        return self.descriptions

    def add_table_entry(self, data):
        if isinstance(data, dict):
            schema_name = data.get('table_schema', '')
            table_name = data.get('table_name', '')
            column_name = data.get('column_name', '')
            udt_name = data.get('udt_name', '')
            position = data.get('ordinal_position', '')
            description = data.get('description', '')

            if not schema_name in self.schema_filter:
                return

            if not self.check_table_filter(table_name):
                return

            path = f"{schema_name}.{table_name}.{column_name}"
            if path in self.descriptions:
                description = self.descriptions[path]

            if not description:
                description = ''

            self.descriptions[path] = description

            add_ids = 1
            if self.add_comment:
                add_ids = 3

            table = self.get_table(schema_name, table_name)
            if table:
                column_id = self.get_next_id(add_ids)
                table.add_column(column_id, column_name, udt_name, position, description)
                self.add_structure_entry(schema_name, table_name, column_name, column_id, description)

    def add_constraint(self, data):
        if isinstance(data, dict):
            constraint_type = data.get('constraint_type')
            table_schema = data.get('table_schema')
            constraint_name = data.get('constraint_name')
            table_name = data.get('table_name')
            column_name = data.get('column_name')
            foreign_table_schema = data.get('foreign_table_schema')
            foreign_table_name = data.get('foreign_table_name')
            foreign_column_name = data.get('foreign_column_name')

            if not table_schema in self.schema_filter:
                return

            if not foreign_table_schema in self.schema_filter:
                return

            if not self.check_table_filter(table_name):
                return

            if not self.check_table_filter(foreign_table_name):
                return

            if self.use_layers:
                if not 'constraints' in self.layers:
                    self.layers['constraints'] = self.get_next_id()

                parent_id = self.layers['constraints']

            else:
                parent_id = self.layers['main']

            table = self.get_table(table_schema, table_name)
            if table:
                table.set_column_type(column_name, constraint_type)

            if constraint_type == 'FOREIGN KEY':
                source_id = self.get_structure_entry(table_schema, table_name, column_name)
                target_id = self.get_structure_entry(foreign_table_schema, foreign_table_name, foreign_column_name)

                if not source_id or not target_id:
                    return

                self.connections.append({
                    'parent_id': parent_id,
                    'id': self.get_next_id(),
                    'source_id': source_id,
                    'target_id': target_id
                })

                if table:
                    table.set_priority(+1)

                foreign_table = self.get_table(foreign_table_schema, foreign_table_name)
                if foreign_table:
                    foreign_table.set_priority(-1)

                if table and foreign_table:
                    foreign_table.add_child(table)
                    table.add_parent(foreign_table)
                    

    def add_diagram_constraint(self, diagram, constraint):
        source_id = constraint['source_id']
        target_id = constraint['target_id']
        parent_id = constraint['parent_id']
        id = constraint['id']

        if source_id in self.structure_ids and target_id in self.structure_ids:

            source_table = self.get_table(self.structure_ids[source_id]['schema'], self.structure_ids[source_id]['table'])
            target_table = self.get_table(self.structure_ids[target_id]['schema'], self.structure_ids[target_id]['table'])

            if source_table and target_table:
                source_column = source_table.get_column()
                target_column = target_table.get_column()

                if source_column == target_column:
                    entry_x = 0
                    exit_x = 0

                elif source_column > target_column:
                    entry_x = 1
                    exit_x = 0

                else:
                    entry_x = 0
                    exit_x = 1

            diagram.append(str(DiagramConstraint(self.config, parent_id, id, source_id, target_id, entry_x, exit_x)))

    def add_diagram_table(self, diagram, table, columns, column, force_direction = None):
        already_used = table.get_used_in_diagram()
        if already_used:
            return None

        if column < 0:
            column = 0

        table.set_used_in_diagram()

        table_width = int(self.config.get('diagram', 'tableWidth', 200)) + 140
        if self.add_comment:
            comment_width = int(self.config.get('diagram', 'commentWidth', 200)) + 40
            table_width = table_width + comment_width

        x = 100 + column * table_width

        table_height = (table.get_column_counts() * 26) + 78

        direction = None

        if self.change_placement_direction:
            if force_direction:
                direction = force_direction
            else:
                if columns[column][2] == 'down':
                    columns[column][2] = 'up'

                else:
                    columns[column][2] = 'down'
        else:
            direction = 'down'

        if direction == 'down':
            y = columns[column][1]
            columns[column][1] = columns[column][1] + table_height

        else:
            y = columns[column][0] - table_height
            columns[column][0] = y

        table.set_position(x, y)

        table.set_column(column)
        diagram.append(str(table))

        child_column_offset = 2

        switch_directions = False
        child_direction = direction
        table_childs = table.get_childs()

        if self.child_priority_fix:
            table_childs = sorted(table_childs, key=lambda table: table.get_priority())

        for table_child in table_childs:

            if self.child_offset_fix and child_column_offset == 2 and len(table_child.get_parents()) > 0 and len(table_child.get_childs()) == 0:
                column_offset = column + 1
            else:
                column_offset = column + child_column_offset

            if not switch_directions and columns[column_offset][1] - columns[column_offset][0] == 0:
                switch_directions = True

            self.add_diagram_table(diagram, table_child, columns, column_offset, child_direction)
            
            if columns[column_offset][1] - columns[column_offset][0] > self.column_max_height:
                child_column_offset = child_column_offset + 1

            if self.change_placement_direction and switch_directions:
                if child_direction == 'up':
                    child_direction = 'down'
                else:
                    child_direction = 'up'

        table_parents = table.get_parents()

        if self.parent_priority_fix:
            table_parents = sorted(table_parents, key=lambda table: table.get_priority())

        for table_parent in table_parents:
            self.add_diagram_table(diagram, table_parent, columns, column - 1, direction)

        return True

    def to_diagram(self):

        columns = []
        for i in range(500):
            columns.append([self.column_center, self.column_center, 'down'])

        column = 0
        last_used_column = None

        diagram = ["""<mxGraphModel><root><mxCell id="0"/>"""]

        for id, layer in enumerate(self.layers):
            diagram.append(str(DiagramLayer(self.layers[layer], layer)))

        sorted_tables = sorted(self.tables, key=lambda table: table.get_priority())

        for table in sorted_tables:
            table_childs = table.get_childs()
            table_parents = table.get_parents()

            if not len(table_childs) and not len(table_parents) and not last_used_column:
                for i, col in enumerate(columns):
                    if col[1] - col[0]:
                        last_used_column = i
                        column = i + 1

            if columns[column][1] - columns[column][0] > self.column_max_height:
                column = column + 1

            added = self.add_diagram_table(diagram, table, columns, column, 'down')
            if not added:
                continue

        for constraint in self.connections:
            self.add_diagram_constraint(diagram, constraint)

        diagram.append("""</root></mxGraphModel>""")
        return '\n'.join(diagram)

def run(args):
    if not args.config:
        sys.stdout.write('ERROR: Missing config file\n')
        sys.exit(1)

    with open(args.config, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    if not 'database' in config:
        sys.stdout.write('ERROR: Missing "database" key in config file\n')
        sys.exit(1)
    else:
        database = config['database']

    if not 'dsn' in database:
        sys.stdout.write('ERROR: Missing "database.dsn" key in config file\n')
        sys.exit(1)

    if not 'type' in database:
        sys.stdout.write('ERROR: Missing "database.type" key in config file\n')
        sys.exit(1)

    dsn = database['dsn']
    type = database['type']
    layer = None

    if type == 'postgresql':
        layer = pgLayer(dsn, config)

    else:
        sys.stdout.write('ERROR: Wrong value for "database.type" key in config file\n')
        sys.exit(1)

    if layer:
        layer.get_table_structure()

        if args.extract:
            config['descriptions'] = layer.get_descriptions()

            with open(args.config, 'w') as yaml_file:
                yaml.safe_dump(config, yaml_file) # Also note the safe_dump

            sys.exit(0)

        layer.get_constraints()

        diagram = str(layer.to_diagram())

        if args.output:
            f = open(args.output, "w")
            if f:
                f.write(diagram)
                f.close()
        else:
            sys.stdout.write(f'{diagram}\n')

def argsError(error):
    pass

def main(argv):
    parser = argparse.ArgumentParser()
    parser.error = argsError

    parser.add_argument('--extract', '-e', action='store_true', default=False, help='Extract descriptions from DB into config file')
    parser.add_argument('--config', '-c', help='Config file')
    parser.add_argument('--output', '-o', help='Output file')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Add more info to output')

    try:
        args = parser.parse_args()
        run (args)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

if __name__ == '__main__':
    main(sys.argv) 

