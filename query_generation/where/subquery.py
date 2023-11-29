import copy
import json
import os
import random

from helper_funcs import get_table_name_from_column, read_random_specs


def generate_subquery(
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery=None,
    query_generator_func=None,
):
    if min_max_depth_in_subquery is None:
        min_max_depth_in_subquery = [0, 0]
    current_dir = os.path.dirname(__file__)
    file_name = os.path.join(current_dir, f"../output/{db_name}.json")

    if subquery_type in ["in_with_subquery", "not_in_with_subquery"]:
        return generate_in_or_not_in_subquery(
            file_name,
            schema,
            schema_types,
            db_name,
            colms,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_func,
        )

    elif subquery_type == "comparison_with_subquery":
        return generate_comparison_subquery(
            file_name,
            schema,
            schema_types,
            db_name,
            colms,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_func,
        )

    elif subquery_type in ["exists_subquery", "not_exists_subquery"]:
        return generate_exists_subquery(
            file_name,
            schema,
            schema_types,
            db_name,
            colms,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_func,
        )

    elif subquery_type == "correlated_subquery":
        return generate_correlated_subquery()

    elif subquery_type == "nested_subquery":
        return generate_nested_subquery()


def generate_in_or_not_in_subquery(
    file_name,
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_func,
):
    in_or_not_in = "NOT IN" if "not" in subquery_type else "IN"
    if random.choice([True, False]):
        in_or_not_in += " ANY" if random.choice([True, False]) else " ALL"

    random_column = random.choice(colms["number"] + colms["text"])
    if "." in random_column:
        random_column = random_column.split(".")[1]
    must_be_in_select = [random_column]
    tables = get_table_name_from_column(random_column, schema)

    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name, db_name, tables, pk, fk, min_max_depth_in_subquery
    )

    dict_spec = {db_name: {spec_hash: spec}}

    merged_queries = query_generator_func(
        schema=schema,
        schema_types=schema_types,
        pk=pk,
        fk=fk,
        specs=dict_spec,
        db_name=db_name,
        must_be_in_select=must_be_in_select,
        write_to_csv=False,
        is_subquery=True,
        testing_with_one_spec=True,
        random_choice=True,
    )

    sub_query = list(merged_queries.values())[0].split("\n")[0]

    where_clause = f"{random_column} {in_or_not_in} ({sub_query})"

    return [where_clause]


def generate_comparison_subquery(
    file_name,
    schema,
    schema_types,
    db_name,
    colms,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_func,
):
    comparison_operators = ["=", "<>", "!=", ">", "<", ">=", "<="]
    modified_by_any_or_all = random.choice([True, False])
    comp_clause = random.choice(comparison_operators)
    if modified_by_any_or_all:
        comp_clause += " ANY" if random.choice([True, False]) else " ALL"

    random_column = random.choice(colms["number"] + colms["text"])
    if "." in random_column:
        random_column = random_column.split(".")[1]
    tables = get_table_name_from_column(random_column, schema)

    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name, db_name, tables, pk, fk, min_max_depth_in_subquery
    )

    if not modified_by_any_or_all:
        agg_func = random.choice(["MAX", "MIN", "AVG", "SUM"])
        must_be_in_select = [f"{agg_func}({random_column})"]
    else:
        must_be_in_select = [random_column]

    dict_spec = {db_name: {spec_hash: spec}}

    merged_queries = query_generator_func(
        schema=schema,
        schema_types=schema_types,
        pk=pk,
        fk=fk,
        specs=dict_spec,
        db_name=db_name,
        must_be_in_select=must_be_in_select,
        write_to_csv=False,
        is_subquery=True,
        testing_with_one_spec=True,
        random_choice=True,
    )

    sub_query = list(merged_queries.values())[0].split("\n")[0]

    where_clause = f"{random_column} {comp_clause} ({sub_query})"

    return [where_clause]


def generate_exists_subquery(
    file_name,
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_func,
):
    exist_or_not_exist = "EXISTS" if "exists" in subquery_type else "NOT EXISTS"

    must_be_in_select = random.choice([["*"], ["1"]])

    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name,
        db_name,
        tables,
        pk,
        fk,
        min_max_depth_in_subquery,
        schema=schema,
        exists=True,
    )
    dict_spec = {db_name: {spec_hash: spec}}

    merged_queries = query_generator_func(
        schema=schema,
        schema_types=schema_types,
        pk=pk,
        fk=fk,
        specs=dict_spec,
        db_name=db_name,
        must_be_in_select=must_be_in_select,
        must_be_in_where=must_be_in_where,
        write_to_csv=False,
        is_subquery=True,
        testing_with_one_spec=True,
        random_choice=True,
    )

    sub_query = list(merged_queries.values())[0].split("\n")[0]

    where_clause = f"{exist_or_not_exist} ({sub_query})"

    return [where_clause]


def generate_correlated_subquery():
    pass


def generate_nested_subquery():
    pass
