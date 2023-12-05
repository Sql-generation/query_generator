import itertools
import random
import sys

import networkx as nx

sys.path.append("..")

import random

from helper_funcs import all_colms
from join import generate_join_query, generate_meaningless_join
from subquery_generator import generate_subquery

from .table_expression_helper_funcs import (
    handle_join_case,
    handle_single_table_case,
    handle_single_table_with_name_changing_case,
    handle_table_expression_for_subquery,
)


def create_table_expression(
    schema,
    pk,
    fk,
    schema_types,
    table_expression_type,
    meaningful_joins,
    db_name,
    random_choice=False,
    query_generator_func=None,
):
    """
    Generate SQL table expression based on the specified type.

    Args:
        schema (dict): Database schema with table names and their columns.
        pk (dict): Primary keys for tables.
        fk (dict): Foreign keys for tables.
        schema_types (dict): Data types of columns in the schema.
        table_expression_type (str or dict): Type of table expression to generate.
        meaningful_joins (str): Type of join - meaningful or meaningless.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: List of queries with attributes based on the specified table expression type.
    """
    case = table_expression_type
    table_expression_query = ""
    queries_with_attributes = []

    if isinstance(
        case, dict
    ):  # case=={"single_table": "city"} mainly used for subquery
        queries_with_attributes = handle_table_expression_for_subquery(
            case, schema, schema_types, random_choice
        )
    elif case == "single_table":  # case=="single_table"
        queries_with_attributes = handle_single_table_case(
            schema, schema_types, random_choice
        )
    elif (
        case == "single_table_with_name_changing"
    ):  # case=="single_table_with_name_changing"
        queries_with_attributes = handle_single_table_with_name_changing_case(
            schema, schema_types, random_choice
        )
    elif "JOIN" in case:  # case=="INNER JOIN" or case=="INNER JOIN_RIGHT JOIN"
        queries_with_attributes = handle_join_case(
            case, schema, fk, schema_types, meaningful_joins, random_choice
        )
    elif case == "CTE":
        pass
    elif case == "subquery":
        from_clauses = generate_subquery(
            schema,
            schema_types,
            db_name,
            None,
            case,
            pk,
            fk,
            # min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_func=query_generator_func,
            having=True,
        )
        print("FROM CLAUSES")
        print(from_clauses)
        queries = []
        for from_clause, tables, attributes in from_clauses:
            print("GHG")
            print(from_clause)
            query = f" FROM {from_clause}"
            # attributes = get_all_attributes_for_from_subquery()
            # queries.append([query, attributes, must_have_attributes])
            # TODO
            queries_with_attributes.append([query, tables, attributes])

    return queries_with_attributes


# Schema and keys
# schema = {
#     "city": [
#         "city_id",
#         "official_name",
#         "status",
#         "area_km_2",
#         "population",
#         "census_ranking",
#     ],
#     "farm": [
#         "farm_id",
#         "year",
#         "total_horses",
#         "working_horses",
#         "total_cattle",
#         "oxen",
#         "bulls",
#         "cows",
#         "pigs",
#         "sheep_and_goats",
#     ],
#     # Other tables and columns...
# }

# primary_keys = {
#     "city": "city_id",
#     "farm": "farm_id",
#     # Other primary keys...
# }

# foreign_keys = {
#     "farm_competition": {"host_city_id": ("city", "city_id")},
#     # Other foreign keys...
# }

# schema_types = {
#     "city": {
#         "city_id": "number",
#         "official_name": "text",
#         # Other column types...
#     },
#     "farm": {
#         "farm_id": "number",
#         "year": "number",
#         # Other column types...
#     },
#     # Other tables and column types...
# }

# Example function calls:
# create_table_expression(schema, primary_keys, foreign_keys, "single_table")
# create_table_expression(schema, primary_keys, foreign_keys, "single_table_with_name_changing")
# create_table_expression(schema, primary_keys, foreign_keys, "multiple_tables")
# create_table_expression(schema, primary_keys, foreign_keys, "sub_query")
