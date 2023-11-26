import json
import os
import random

from helper_funcs import (
    create_graph_from_schema,
    get_corresponding_fk_table,
    get_table_name_from_column,
)


def generate_subquery(
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables,
):
    current_dir = os.path.dirname(__file__)
    file_name = os.path.join(current_dir, f"../output/{db_name}.json")

    if subquery_type == "in_with_subquery" or subquery_type == "not_in_with_subquery":
        # add ALL
        print(subquery_type)
        if random.choice([True, False]):
            in_or_not_in = subquery_type.split("_")[0].upper()
            if in_or_not_in == "NOT":
                in_or_not_in = "NOT IN"
        else:
            if random.choice([True, False]):
                in_or_not_in = subquery_type.split("_")[0].upper() + " ANY "
                if in_or_not_in.startswith("NOT"):
                    in_or_not_in = "NOT IN ANY"
            else:
                in_or_not_in = subquery_type.split("_")[0].upper() + " ALL "
                if in_or_not_in.startswith("NOT"):
                    in_or_not_in = "NOT IN ALL"
        # randomally choose a specification from farm.json

        with open(file_name, "r") as json_file:
            specs = json.load(json_file)
            specs2 = list(specs[db_name])
        # randomally choose a specification from dictionary

        spec_hash = random.choice(specs2)
        spec = specs[db_name][spec_hash]
        # generate a query from the specification
        random_column = random.choice(colms["number"] + colms["text"])
        if "." in random_column:
            random_column = random_column.split(".")[1]
        must_be_in_select = [random_column]
        tables = get_table_name_from_column(random_column, schema)
        if random.choice([True, False]):
            spec["table_exp_type"] = {"single_table": random.choice(tables)}
        else:
            spec["table_exp_type"] = {
                "single_table_with_name_changing": random.choice(tables)
            }
        spec["number_of_value_exp_in_group_by"] = 0
        spec["having_type"] = "none"
        spec["orderby_type"] = "none"

        dict_spec = {}

        dict_spec[db_name] = {spec_hash: spec}
        print(dict_spec)
        print("___________________")
        from query_generator_from_specifications import query_generator

        merged_queries = query_generator(
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

        where_clause = random_column + " " + in_or_not_in + f"({sub_query})"

        return [where_clause]

    elif subquery_type == "comparison_with_subquery":
        print(subquery_type)
        comparison_operators = ["=", "<>", "!=", ">", "<", ">=", "<="]
        modified_by_any_or_all = True
        if random.choice([True, False]):
            comp_clause = random.choice(comparison_operators)
            modified_by_any_or_all = False
        else:
            if random.choice([True, False]):
                comp_clause = random.choice(comparison_operators) + " ANY "
            else:
                comp_clause = random.choice(comparison_operators) + " ALL "
        # randomally choose a specification from farm.json

        with open(file_name, "r") as json_file:
            specs = json.load(json_file)
            specs2 = list(specs[db_name])
        # randomally choose a specification from dictionary

        spec_hash = random.choice(specs2)
        spec = specs[db_name][spec_hash]
        # print(spec)
        # generate a query from the specification
        random_column = random.choice(colms["number"] + colms["text"])
        if "." in random_column:
            random_column = random_column.split(".")[1]
        tables = get_table_name_from_column(random_column, schema)
        if random.choice([True, False]):
            spec["table_exp_type"] = {"single_table": random.choice(tables)}
        else:
            spec["table_exp_type"] = {
                "single_table_with_name_changing": random.choice(tables)
            }
        spec["number_of_value_exp_in_group_by"] = 0
        spec["having_type"] = "none"
        spec["orderby_type"] = "none"
        # "agg_col",
        # "count_distinct_col",
        if not modified_by_any_or_all:
            agg_func = random.choice(["MAX", "MIN", "AVG", "SUM"])
            must_be_in_select = [agg_func + "(" + random_column + ")"]

        else:
            must_be_in_select = [random_column]

        # if spec["number_of_value_exp_in_group_by"] > 1:
        #     if random.choice([True, False]):
        #         spec["number_of_value_exp_in_group_by"] = 0
        #     else:
        #         spec["number_of_value_exp_in_group_by"] = 1

        dict_spec = {}
        dict_spec[db_name] = {spec_hash: spec}
        print(dict_spec)
        from query_generator_from_specifications import query_generator

        merged_queries = query_generator(
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

        where_clause = random_column + " " + comp_clause + f" ({sub_query})"

        return [where_clause]

    elif subquery_type == "exists_subquery" or subquery_type == "not_exists_subquery":
        print(subquery_type)
        if subquery_type == "exists_subquery":
            exist_or_not_exist = "EXISTS"
        else:
            exist_or_not_exist = "NOT EXISTS"

        must_be_in_select = random.choice([["*"], ["1"]])

        # randomally choose a specification from farm.json

        with open(file_name, "r") as json_file:
            specs = json.load(json_file)
            specs2 = list(specs[db_name])
        # randomally choose a specification from dictionary
        spec_hash = random.choice(specs2)
        spec = specs[db_name][spec_hash]
        print(spec)
        # generate a query from the specification
        if isinstance(tables, dict):
            table = list(tables.keys())[0]
            pk_of_table = pk[tables[table]]
        else:
            table = random.choice(tables)
            pk_of_table = pk[table]
        join_definitions = create_graph_from_schema(schema, fk)
        if isinstance(tables, dict):
            possible_table_keys = get_corresponding_fk_table(
                tables[table], join_definitions
            )
        else:
            possible_table_keys = get_corresponding_fk_table(table, join_definitions)
        if len(possible_table_keys) == 0:
            return ""
        else:
            random_table_key = random.choice(possible_table_keys)
            must_be_in_where = [table + "." + pk_of_table + " = ", random_table_key[1]]
            if random.choice([True, False]):
                spec["table_exp_type"] = {"single_table": random_table_key[0]}
            else:
                spec["table_exp_type"] = {
                    "single_table_with_name_changing": random_table_key[0]
                }
        spec["number_of_value_exp_in_group_by"] = 0
        spec["having_type"] = "none"
        spec["orderby_type"] = "none"

        dict_spec = {}
        dict_spec[db_name] = {spec_hash: spec}
        print(dict_spec)
        from query_generator_from_specifications import query_generator

        merged_queries = query_generator(
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

        where_clause = exist_or_not_exist + f" ({sub_query})"
        print(where_clause)
        return [where_clause]

    elif subquery_type == "correlated_subquery":
        pass
    elif subquery_type == "nested_subquery":
        pass
