import itertools
import random
import sys

import networkx as nx

sys.path.append("..")

from helper_funcs import all_colms
from join import generate_join_query, generate_meaningless_join


def create_table_expression(
    schema,
    pk,
    fk,
    schema_types,
    table_expression_type,
    meaningful_joins,
    random_choice=False,
):
    """
    Generate SQL table expression based on the specified type.

    Args:
    - schema (dict): Database schema with table names and their columns.
    - pk (dict): Primary keys for tables.
    - fk (dict): Foreign keys for tables.
    - schema_types (dict): Data types of columns in the schema.
    - table_expression_type (str or dict): Type of table expression to generate.
    - meaningful_joins (str): Type of join - meaningful or meaningless.

    Returns:
    - list: List of queries with attributes based on the specified table expression type.
    """

    case = table_expression_type
    table_expression_query = ""
    queries_with_attributes = []

    # Generate table expression based on different cases
    if isinstance(case, dict):  # case=={"single_table": "city"}
        type_of_table = list(case.keys())[0]
        table = case[type_of_table]

        if type_of_table == "single_table":
            # Single table expression
            table_expression_query = f" FROM {table}"
            attributes = all_colms(schema, schema_types, [table])
            queries_with_attributes.append(
                [table_expression_query, [table], attributes]
            )

        elif type_of_table == "single_table_with_name_changing":
            # Single table expression with alias
            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
            table_expression_query = f" FROM {table} AS {alias_name}"
            attributes = all_colms(schema, schema_types, [table], [alias_name])

            queries_with_attributes.append(
                [table_expression_query, {alias_name: table}, attributes]
            )

    if case == "single_table":
        # Iterate through all tables as single table expressions
        if random_choice:
            table = random.choice(list(schema.keys()))
            table_expression_query = f" FROM {table}"
            attributes = all_colms(schema, schema_types, [table])
            queries_with_attributes.append(
                [table_expression_query, [table], attributes]
            )
        else:
            for table in schema:
                table_expression_query = f" FROM {table}"
                attributes = all_colms(schema, schema_types, [table])
                queries_with_attributes.append(
                    [table_expression_query, [table], attributes]
                )

    elif case == "single_table_with_name_changing":
        # Iterate through all tables with an alias as single table expressions
        if random_choice:
            table = random.choice(list(schema.keys()))
            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
            table_expression_query = f" FROM {table} AS {alias_name}"
            attributes = all_colms(schema, schema_types, [table], [alias_name])

            queries_with_attributes.append(
                [table_expression_query, {alias_name: table}, attributes]
            )
        else:
            for table in schema:
                alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                table_expression_query = f" FROM {table} AS {alias_name}"
                attributes = all_colms(schema, schema_types, [table], [alias_name])

                queries_with_attributes.append(
                    [table_expression_query, {alias_name: table}, attributes]
                )

    elif "JOIN" in case:
        # Generate join queries based on the specified join types
        num_joins = len(case.split("_"))
        join_types = case.split("_")

        if meaningful_joins == "yes":
            # Generate meaningful join queries
            temp_queries = generate_join_query(
                schema, fk, join_types, random_choice=random_choice
            )
            for temp in temp_queries:
                all_columns = all_colms(schema, schema_types, temp[1])
                temp.append(all_columns)
                queries_with_attributes.append(temp)

        elif meaningful_joins == "no":
            # Generate meaningless join queries
            temp_queries = generate_meaningless_join(
                schema, int(num_joins), join_types, random_choice=random_choice
            )
            for temp in temp_queries:
                all_columns = all_colms(schema, schema_types, temp[1])
                queries_with_attributes.append([temp[0], temp[1], all_columns])

        else:
            if random_choice:
                meaningful_joins = "no"
                temp_queries = generate_meaningless_join(
                    schema, int(num_joins), join_types, random_choice=random_choice
                )
                for temp in temp_queries:
                    all_columns = all_colms(schema, schema_types, temp[1])
                    queries_with_attributes.append([temp[0], temp[1], all_columns])
            else:
                # Generate both meaningful and meaningless join queries
                temp_queries = generate_meaningless_join(
                    schema, int(num_joins), join_types, random_choice=random_choice
                )
                for temp in temp_queries:
                    all_columns = all_colms(schema, schema_types, temp[1])
                    queries_with_attributes.append([temp[0], temp[1], all_columns])

                temp_queries = generate_join_query(
                    schema, fk, join_types, random_choice=random_choice
                )
                for temp in temp_queries:
                    all_columns = all_colms(schema, schema_types, temp[1])
                    temp.append(all_columns)
                    queries_with_attributes.append(temp)

    elif case == "CTE":
        pass

    elif case == "sub_query":
        pass

    return queries_with_attributes


# Schema and keys
schema = {
    "city": [
        "city_id",
        "official_name",
        "status",
        "area_km_2",
        "population",
        "census_ranking",
    ],
    "farm": [
        "farm_id",
        "year",
        "total_horses",
        "working_horses",
        "total_cattle",
        "oxen",
        "bulls",
        "cows",
        "pigs",
        "sheep_and_goats",
    ],
    # Other tables and columns...
}

primary_keys = {
    "city": "city_id",
    "farm": "farm_id",
    # Other primary keys...
}

foreign_keys = {
    "farm_competition": {"host_city_id": ("city", "city_id")},
    # Other foreign keys...
}

schema_types = {
    "city": {
        "city_id": "number",
        "official_name": "text",
        # Other column types...
    },
    "farm": {
        "farm_id": "number",
        "year": "number",
        # Other column types...
    },
    # Other tables and column types...
}

# Example function calls:
# create_table_expression(schema, primary_keys, foreign_keys, "single_table")
# create_table_expression(schema, primary_keys, foreign_keys, "single_table_with_name_changing")
# create_table_expression(schema, primary_keys, foreign_keys, "multiple_tables")
# create_table_expression(schema, primary_keys, foreign_keys, "sub_query")
