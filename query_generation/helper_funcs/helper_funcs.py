import csv
import hashlib
import itertools

#     return colms
import json
import os
import random


def read_random_specs(
    file_name,
    db_name,
    tables,
    pk,
    fk,
    min_max_depth_in_subquery,
    schema=None,
    exists=False,
):
    """
    Read a random specification from a file and generate a query based on the specification.

    Args:
        file_name (str): Name of the file containing the specifications.
        db_name (str): Name of the database.
        tables (list or dict): List or dictionary of table names.
        pk (dict): Dictionary mapping table names to their primary key columns.
        fk (list): List of join definitions, each containing "table1", "table2", "first_key", and "second_key".
        min_max_depth_in_subquery (list): List containing the minimum and maximum depth of nested subqueries.
        schema (dict, optional): Dictionary containing the schema information. Defaults to None.
        exists (bool, optional): Flag indicating whether the query should use EXISTS subqueries. Defaults to False.

    Returns:
        tuple: Tuple containing the generated specification, specification hash, and must-be-in-where condition.

    Examples:
        >>> file_name = "specs.json"
        >>> db_name = "mydb"
        >>> tables = ["table1", "table2"]
        >>> pk = {"table1": "pk1", "table2": "pk2"}
        >>> fk = [
        ...     {"table1": "table1", "table2": "table2", "first_key": "fk1", "second_key": "pk1"},
        ...     {"table1": "table2", "table2": "table3", "first_key": "fk2", "second_key": "pk2"}
        ... ]
        >>> min_max_depth_in_subquery = [2, 4]
        >>> schema = {
        ...     "table1": ["col1", "col2"],
        ...     "table2": ["col3", "col4"],
        ...     "table3": ["col5", "col6"]
        ... }
        >>> exists = True
        >>> read_random_specs(file_name, db_name, tables, pk, fk, min_max_depth_in_subquery, schema, exists)
        ({...}, 'spec_hash', ['table1.pk1 = ', 'pk1'])
    """
    must_be_in_where = None
    with open(file_name, "r") as json_file:
        specs = json.load(json_file)
        specs2 = list(specs[db_name])

    spec_hash = random.choice(specs2)
    spec = specs[db_name][spec_hash]

    if exists:
        table, pk_of_table = get_random_table_and_pk(tables, pk)
        join_definitions = create_graph_from_schema(schema, fk)
        possible_table_keys = get_corresponding_fk_table(table, join_definitions)
        if len(possible_table_keys) == 0:
            return "", "", None
        random_table_key = random.choice(possible_table_keys)
        must_be_in_where = [f"{table}.{pk_of_table} = ", random_table_key[1]]
        spec["table_exp_type"] = (
            {"single_table": random_table_key[0]}
            if random.choice([True, False])
            else {"single_table_with_name_changing": random_table_key[0]}
        )
    elif random.choice([True, False]):
        spec["table_exp_type"] = {"single_table": random.choice(tables)}
    else:
        spec["table_exp_type"] = {
            "single_table_with_name_changing": random.choice(tables)
        }

    spec["number_of_value_exp_in_group_by"] = 0
    spec["having_type"] = "none"
    spec["orderby_type"] = "none"

    if min_max_depth_in_subquery[0] > 0:
        spec["min_max_depth_in_subquery"] = [
            min_max_depth_in_subquery[0] - 1,
            min_max_depth_in_subquery[1] - 1,
        ]
        subquery_type = random.choice(
            [
                "in_with_subquery",
                "not_in_with_subquery",
                "exists_subquery",
                "not_exists_subquery",
                "comparison_with_subquery",
            ]
        )
        spec["where_type"] = subquery_type
    elif min_max_depth_in_subquery[1] > 0:
        if spec["where_type"] in [
            "in_with_subquery",
            "not_in_with_subquery",
            "exists_subquery",
            "not_exists_subquery",
            "comparison_with_subquery",
        ]:
            spec["min_max_depth_in_subquery"] = [
                min_max_depth_in_subquery[0],
                min_max_depth_in_subquery[1] - 1,
            ]
            min_max_depth_in_subquery[1] -= 1
    elif min_max_depth_in_subquery[1] == 0:
        if spec["where_type"] in [
            "in_with_subquery",
            "not_in_with_subquery",
            "exists_subquery",
            "not_exists_subquery",
            "comparison_with_subquery",
        ]:
            min_max_depth_in_subquery = [0, 0]
            read_random_specs(file_name, db_name, tables, pk, min_max_depth_in_subquery)
            spec["min_max_depth_in_subquery"] = [0, 0]

    return spec, spec_hash, must_be_in_where


def get_random_table_and_pk(tables, pk):
    """
    Get a random table name and its corresponding primary key column.

    Args:
        tables (list or dict): List or dictionary of table names.
        pk (dict): Dictionary mapping table names to their primary key columns.

    Returns:
        tuple: Tuple containing the random table name and its corresponding primary key column.

    Examples:
        >>> tables = ["table1", "table2"]
        >>> pk = {"table1": "pk1", "table2": "pk2"}
        >>> get_random_table_and_pk(tables, pk)
        ('table1', 'pk1')
    """
    if isinstance(tables, dict):
        table = list(tables.keys())[0]
        pk_of_table = pk[tables[table]]
    else:
        table = random.choice(tables)
        pk_of_table = pk[table]
    return table, pk_of_table


def get_attributes_ends_with(name, attributes):
    """
    Get the attribute that ends with the given name from the provided attributes.

    Args:
        name (str): Ending of the attribute name to search for.
        attributes (dict): Dictionary containing the attributes, with keys "number" and "text" representing different types.

    Returns:
        str or None: Attribute name that ends with the given name, or None if not found.

    Examples:
        >>> attributes = {"number": ["col1", "col2", "col3"], "text": ["col4", "col5"]}
        >>> get_attributes_ends_with("3", attributes)
        'col3'

        >>> get_attributes_ends_with("6", attributes)
        None
    """
    attrs = attributes["number"] + attributes["text"]
    return next((attribute for attribute in attrs if attribute.endswith(name)), None)


def get_corresponding_fk_table(table_of_pk, join_definitions):
    """
    Get the possible tables and their corresponding foreign keys associated with a given primary key table.

    Args:
        table_of_pk (str): Table name of particular primary key .
        join_definitions (list): List of join definitions, each containing "table1", "table2", "first_key", and "second_key".

    Returns:
        list: List of lists, where each inner list contains the possible table name and its corresponding foreign key.

    Examples:
        >>> join_definitions = [
        ...     {"table1": "table1", "table2": "table2", "first_key": "fk1", "second_key": "pk1"},
        ...     {"table1": "table2", "table2": "table3", "first_key": "fk2", "second_key": "pk2"}
        ... ]
        >>> get_corresponding_fk_table("table1", join_definitions)
        [['table2', 'pk1']]

        >>> get_corresponding_fk_table("table2", join_definitions)
        [['table1', 'fk1'], ['table3', 'pk2']]
    """
    possible_tables_and_pk = []
    for pairs in join_definitions:
        if pairs["table1"] == table_of_pk:
            possible_tables_and_pk.append([pairs["table2"], pairs["second_key"]])
        elif pairs["table2"] == table_of_pk:
            possible_tables_and_pk.append([pairs["table1"], pairs["first_key"]])
    return possible_tables_and_pk


def write_queries_to_file(merged_queries):
    """
    Write the merged queries to a CSV file.

    Args:
        merged_queries (dict): Dictionary containing the merged queries, with specification as keys and partial queries as values.

    Returns:
        None
    """
    current_dir = os.path.dirname(__file__)
    output_dir = os.path.abspath(os.path.join(current_dir, "../output"))
    csv_file = os.path.join(output_dir, "res.csv")
    with open(csv_file, mode="w", newline="") as csv_file:
        fieldnames = ["Specification", "Partial Query"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for spec, merged_query in merged_queries.items():
            query_data = {
                "Specification": spec,
                "Partial Query": merged_query,
            }
            writer.writerow(query_data)


def get_table_name_from_column(column, schema):
    """
    Get the table name(s) associated with a given column in the schema.

    Args:
        column (str): Column name.
        schema (dict): Dictionary containing the schema information.

    Returns:
        list: List of table names associated with the column.

    Examples:
        >>> schema = {
        ...     "table1": ["col1", "col2"],
        ...     "table2": ["col3", "col4"]
        ... }
        >>> get_table_name_from_column("col1", schema)
        ['table1']

        >>> get_table_name_from_column("table2.col3", schema)
        ['table2']
    """
    tables = []
    col = column
    if column.find(".") != -1:
        tables.append(column.split(".")[0])
        col = column.split(".")[1]

    for table in schema:
        if col in schema[table] and table not in tables:
            tables.append(table)
    return tables


def generate_arithmetic_expression(attributes, num_parts=None, max_depth=3):
    """
    Generate a random arithmetic expression using the given attributes.

    Args:
        attributes (dict): Dictionary containing the attributes, with keys "number" representing numeric columns.
        num_parts (int, optional): Number of parts in the arithmetic expression. Defaults to None.
        max_depth (int, optional): Maximum depth of nested arithmetic expressions. Defaults to 3.

    Returns:
        str: Generated arithmetic expression.

    Examples:
        >>> attributes = {"number": ["col1", "col2", "col3"]}
        >>> generate_arithmetic_expression(attributes)
        'col1 + col2 * col3'

        >>> attributes = {"number": ["col1", "col2", "col3"]}
        >>> generate_arithmetic_expression(attributes, num_parts=2, max_depth=2)
        'col1 * (col2 - col3)'
    """
    if num_parts is None:
        num_parts = random.randint(1, 2)

    arithmetic_expression = []
    numeric_attributes = attributes["number"]
    for i in range(num_parts):
        if max_depth <= 0:
            break

        random_part_of_arithmetic_expression = random.choice(
            [
                "constant",
                "column",
                "arithmetic_expression_with_par",
            ]
        )

        if random_part_of_arithmetic_expression == "constant":
            random_constant = random.randint(1, 100)
            arithmetic_expression.append(str(random_constant))

        elif random_part_of_arithmetic_expression == "column":
            random_column = random.choice(numeric_attributes)
            arithmetic_expression.append(
                generate_column_expression(random_column, attributes, max_depth)
            )

        elif random_part_of_arithmetic_expression == "arithmetic_expression_with_par":
            random_number_of_parts = random.randint(2, 3)
            random_arithmetic_expression = generate_arithmetic_expression(
                attributes, num_parts=random_number_of_parts, max_depth=max_depth - 1
            )
            arithmetic_expression.append(f"({random_arithmetic_expression})")

        if i != num_parts - 1:
            random_arithmetic_expression = random.choice(["+", "-", "*", "/"])
            arithmetic_expression.append(random_arithmetic_expression)

    return " ".join(arithmetic_expression)


def generate_column_expression(column, attributes, max_depth):
    """
    Generate a column expression based on the given column and attributes.

    Args:
        column (str): Column name.
        attributes (dict): Dictionary containing the attributes, with keys "number" representing numeric columns.
        max_depth (int): Maximum depth of nested arithmetic expressions.

    Returns:
        str: Generated column expression.
    """
    if random.choice([True, False]):
        return column

    random_math_func = random.choice(
        [
            "ABS",
            "CEILING",
            "FLOOR",
            "ROUND",
            "EXP",
            "POWER",
            "SQRT",
            "LOG",
            "LOG10",
            "RAND",
            "SIGN",
        ]
    )

    if random_math_func in ["POWER", "ROUND"]:
        return f"{random_math_func}({column},{random.randint(2,5)})"
    random_number_of_parts = random.randint(1, 3)
    random_arithmetic_expression = generate_arithmetic_expression(
        attributes, num_parts=random_number_of_parts, max_depth=max_depth - 1
    )
    return f"{random_math_func}({random_arithmetic_expression})"


def write_hash_table_to_json(hash_table, file_name):
    """
    Write the hash table to a JSON file.

    Args:
        hash_table (dict): Dictionary containing the hash table to be written to the JSON file.
        file_name (str): Name of the JSON file to write the hash table to.

    Returns:
        None
    """
    json_object = json.dumps(hash_table, indent=4)
    with open(file_name, "w") as outfile:
        outfile.write(json_object)


import hashlib
import json


def calculate_hash(d):
    """
    Calculate the SHA-1 hash of a dictionary.

    Args:
        d (dict): Dictionary to calculate the hash for.

    Returns:
        str: SHA-1 hash value as a hexadecimal string.

    Examples:
        >>> data = {"key1": "value1", "key2": "value2"}
        >>> calculate_hash(data)
        '0a4d55a8d778e5022fab701977c5d840bbc486d0'
    """
    json_str = json.dumps(d, sort_keys=True)
    hash_object = hashlib.sha1(json_str.encode())
    return hash_object.hexdigest()


def write_detail_to_json(details, file_name):
    """
    Write the details to a JSON file.

    Args:
        details (dict): Dictionary containing the details to be written to the JSON file.
        file_name (str): Name of the JSON file to write the details to.

    Returns:
        None
    """
    json_object = json.dumps(details, indent=4)
    with open(file_name, "w") as outfile:
        outfile.write(json_object)


def create_graph_from_schema(schema, fk):
    """
    Create a graph representation of the foreign key relationships from the given schema.

    Args:
        schema (dict): Dictionary containing the schema information.
        fk (dict): Dictionary mapping tables to their foreign key relationships.

    Returns:
        list: List of dictionaries representing the graph edges, where each dictionary contains the following keys:
            - "table1": Name of the first table.
            - "table2": Name of the second table.
            - "first_key": Foreign key column in the first table.
            - "second_key": Corresponding primary key column in the second table.

    Examples:
        >>> schema = {
        ...     "table1": ["col1", "col2"],
        ...     "table2": ["col3", "col4"]
        ... }
        >>> fk = {
        ...     "table1": {
        ...         "fk1": ("table2", "pk1"),
        ...         "fk2": ("table2", "pk2")
        ...     }
        ... }
        >>> create_graph_from_schema(schema, fk)
        [
            {'table1': 'table1', 'table2': 'table2', 'first_key': 'fk1', 'second_key': 'pk1'},
            {'table1': 'table1', 'table2': 'table2', 'first_key': 'fk2', 'second_key': 'pk2'}
        ]
    """
    graph = []
    for table in fk:
        fk_for_tables = list(fk[table].keys())
        graph.extend(
            {
                "table1": table,
                "table2": table2_with_key[0],
                "first_key": fk_for_tables[i],
                "second_key": table2_with_key[1],
            }
            for i, table2_with_key in enumerate(fk[table].values())
        )
    return graph


def all_colms(schema, schema_type, unique_tables, alias=None):
    """
    Generate a dictionary of columns categorized as "number" or "text" based on the provided schema and schema_type.

    Args:
        schema (dict): Dictionary containing the schema information.
        schema_type (dict): Dictionary mapping column names to their corresponding types.
        unique_tables (list): List of unique table names.
        alias (list, optional): List of table aliases. Defaults to None.

    Returns:
        dict: Dictionary containing the categorized columns.

    Examples:
        >>> schema = {
        ...     "table1": ["col1", "col2"],
        ...     "table2": ["col3", "col4"]
        ... }
        >>> schema_type = {
        ...     "table1": {"col1": "number", "col2": "text"},
        ...     "table2": {"col3": "number", "col4": "text"}
        ... }
        >>> unique_tables = ["table1", "table2"]
        >>> alias = ["alias1", "alias2"]
        >>> all_colms(schema, schema_type, unique_tables, alias)
        {'number': ['alias1.col1', 'alias2.col3'], 'text': ['alias1.col2', 'alias2.col4']}
    """
    columns = {"number": [], "text": []}

    if len(unique_tables) == 1:
        table = unique_tables[0]
        for col_name in schema[table]:
            if schema_type[table][col_name] == "number":
                columns["number"].append(
                    f"{alias[0]}.{col_name}" if alias else col_name
                )
            elif schema_type[table][col_name] == "text":
                columns["text"].append(f"{alias[0]}.{col_name}" if alias else col_name)
    else:
        column_names = {}
        for table in unique_tables:
            for col_name in schema[table]:
                full_col_name = f"{table}.{col_name}"
                column_names.setdefault(col_name, []).append(full_col_name)

        for col_name, tables in column_names.items():
            formatted_cols = tables if len(tables) > 1 else [tables[0].split(".")[1]]
            columns[schema_type[tables[0].split(".")[0]][col_name]].extend(
                formatted_cols
            )

    return columns


def random_not_pk_cols(attributes, unique_tables, pk, number_of_col):
    """
    Generate random combinations of non-primary key columns from the given attributes and tables.

    This function is used for getting combinations of non-primary key columns to be used in the GROUP BY clause.

    Args:
        attributes (dict): Dictionary containing the attributes, with keys "number" and "text" representing different types of columns.
        unique_tables (list or str or dict): Either a list of tables, a string representing a single table, or a dictionary of table aliases and their corresponding tables.
        pk (dict): Dictionary mapping tables to their primary key columns.
        number_of_col (int): The number of columns to include in each combination.

    Returns:
        list: List of generated random combinations of non-primary key columns.

    Raises:
        ValueError: If the number of available columns is less than the desired number of columns.

    Examples:
        >>> attributes = {"number": ["col1", "col2"], "text": ["col3", "col4"]}
        >>> unique_tables = ["table1", "table2"]
        >>> pk = {"table1": "pk1", "table2": "pk2"}
        >>> number_of_col = 2
        >>> random_not_pk_cols(attributes, unique_tables, pk, number_of_col)
        [['col1', 'col3'], ['col1', 'col4'], ['col2', 'col3'], ['col2', 'col4']]

        >>> attributes = {"number": ["col1", "col2"], "text": ["col3", "col4"]}
        >>> unique_tables = "table1"
        >>> pk = {"table1": "pk1"}
        >>> number_of_col = 3
        >>> random_not_pk_cols(attributes, unique_tables, pk, number_of_col)
        [['col1', 'col2', 'col3'], ['col1', 'col2', 'col4']]
    """
    all_cols = attributes["number"] + attributes["text"]

    if isinstance(unique_tables, list):
        for table in unique_tables:
            if pk[table] in all_cols:
                all_cols.remove(pk[table])
            if f"{table}.{pk[table]}" in all_cols:
                all_cols.remove(f"{table}.{pk[table]}")
    elif isinstance(unique_tables, str):
        if pk[unique_tables] in all_cols:
            all_cols.remove(pk[unique_tables])
    else:
        for alias_table in unique_tables:
            if f"{alias_table}.{pk[unique_tables[alias_table]]}" in all_cols:
                all_cols.remove(f"{alias_table}.{pk[unique_tables[alias_table]]}")

    if len(all_cols) < number_of_col:
        raise ValueError(
            "Number of available columns is less than the desired number of columns"
        )

    sample_sets = []
    for _ in range(2):
        sample_set = random.sample(all_cols, number_of_col)
        while sample_set in sample_sets:
            sample_set = random.sample(all_cols, number_of_col)
        sample_sets.append(sample_set)

    return sample_sets


def select_combinations(elements_list, num_combinations):
    """
    Generate unique combinations of elements from the provided list.

    Args:
        elements_list (list): List of elements.
        num_combinations (int): Number of elements in each combination.

    Returns:
        list: List of unique combinations of elements.

    Examples:
        >>> elements_list = ["A", "B", "C"]
        >>> num_combinations = 2
        >>> select_combinations(elements_list, num_combinations)
        [['A', 'A'], ['A', 'B'], ['A', 'C'], ['B', 'B'], ['B', 'C'], ['C', 'C']]

        >>> elements_list = ["A", "B"]
        >>> num_combinations = 3
        >>> select_combinations(elements_list, num_combinations)
        [['A', 'A', 'A'], ['A', 'A', 'B'], ['A', 'B', 'B'], ['B', 'B', 'B']]
    """
    combinations = list(itertools.product(elements_list, repeat=num_combinations))
    unique_combinations = {tuple(sorted(combination)) for combination in combinations}
    return [list(combination) for combination in unique_combinations]


def print_attributes(**kwargs):
    """
    Print the key-value pairs of the provided attributes.

    Args:
        **kwargs: Key-value pairs of attributes.

    Examples:
        >>> print_attributes(name="John", age=30, city="New York")
        name: John
        age: 30
        city: New York

        >>> print_attributes(color="blue", size="large")
        color: blue
        size: large
    """
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    print()


schema = {
    "city": [
        "City_ID",
        "Official_Name",
        "Status",
        "Area_km_2",
        "Population",
        "Census_Ranking",
    ],
    "farm": [
        "Farm_ID",
        "Year",
        "Total_Horses",
        "Working_Horses",
        "Total_Cattle",
        "Oxen",
        "Bulls",
        "Cows",
        "Pigs",
        "Sheep_and_Goats",
    ],
    "farm_competition": ["Competition_ID", "Year", "Theme", "Host_city_ID", "Hosts"],
    "competition_record": ["Competition_ID", "Farm_ID", "Rank"],
}
schema_types = {
    "city": {
        "City_ID": "number",
        "Official_Name": "text",
        "Status": "text",
        "Area_km_2": "number",
        "Population": "number",
        "Census_Ranking": "text",
    },
    "farm": {
        "Farm_ID": "number",
        "Year": "number",
        "Total_Horses": "number",
        "Working_Horses": "number",
        "Total_Cattle": "number",
        "Oxen": "number",
        "Bulls": "number",
        "Cows": "number",
        "Pigs": "number",
        "Sheep_and_Goats": "number",
    },
    "farm_competition": {
        "Competition_ID": "number",
        "Year": "number",
        "theme": "text",
        "Host_City_ID": "number",
        "hosts": "text",
    },
    "competition_record": {
        "Competition_Id": "number",
        "Farm_ID": "number",
        "Rank": "number",
    },
}
foreign_keys = {
    "farm_competition": {"Host_City_ID": ("city", "City_ID")},
    "competition_record": {
        "Farm_ID": ("farm", "Farm_ID"),
        "Competition_ID": ("farm_competition", "Competition_ID"),
    },
}
fk = {
    "farm_competition": {"Host_City_ID": ("city", "City_ID")},
    "competition_record": {
        "Farm_ID": ("farm", "Farm_ID"),
        "Competition_ID": ("farm_competition", "Competition_ID"),
    },
}
temp_queries = "FROM city JOIN competition_record JOIN farm JOIN farm_competition ON farm_competition.host_city_id = city.city_id AND competition_record.farm_id = farm.farm_id AND competition_record.competition_id = farm_competition.competition_id"
# all_columns = all_colms(schema, schema_types, ["city"], ["m"])
# print(all_columns)
# print(create_graph_from_schema(schema, fk))

# # Example usage:
# attributes = {"number": ["col1", "col2", "col3"]}
# expression = generate_arithmetic_expression(attributes, 1)
