import json
import os

from helper_funcs import calculate_hash, select_combinations, write_hash_table_to_json
from join import get_max_joins_and_join_definitions
from read_schema.read_schema import convert_json_to_schema

current_dir = os.path.dirname(__file__)
file_name = os.path.join(current_dir, "../spider/tables.json")
all_db = convert_json_to_schema(file_name)

import random


def complete_specs(db_file, config_file, db_name=None):
    """
    Generate specifications for queries based on the given database schema and configuration.

    Args:
        db_file (str): The path to the JSON file containing the database schema.
        config_file (str): The path to the JSON file containing the configuration for generating specifications.
        db_name (str, optional): The name of the specific database to generate specifications for. Defaults to None.

    Returns:
        None
    """
    all_db = convert_json_to_schema(db_file)
    specs = {}

    with open(config_file, "r") as f:
        spec_config = json.load(f)

    if db_name:
        print(db_name)
        specs[db_name] = generate_specifications_for_queries(
            all_db[db_name]["schema"],
            all_db[db_name]["foreign_keys"],
            spec_config,
        )

        current_dir = os.path.dirname(__file__)
        file_name = os.path.join(current_dir, f"output/{db_name}.json")
        write_hash_table_to_json(specs, file_name)
    else:
        for db in all_db:
            specs[db_name] = generate_specifications_for_queries(
                all_db[db_name]["schema"],
                all_db[db_name]["foreign_keys"],
                spec_config,
            )

            current_dir = os.path.dirname(__file__)
            file_name_for_write = os.path.join(current_dir, f"output/{db}.json")
            write_hash_table_to_json(specs[db], file_name_for_write)


def generate_specifications_for_queries(schema, foreign_keys, specs, num=100):
    set_ops_types = specs["set_op_types"]
    first_spec = generate_specifications_for_queries_without_set_ops(
        schema, foreign_keys, specs["first_query"], num
    )
    # generate_hash_table(
    #     num,
    #     table_exp_types_with_types_of_joins,
    #     completed_specifications["where_clause_types"],
    #     number_of_valu_exps_in_group_by,
    #     having_types_without_having_group_by,
    #     having_types_with_having_group_by,
    #     orderby_types,
    #     limit_types,
    #     meaningful_joins,
    #     distinct_types,
    #     all_value_exp_types,
    #     min_max_depth_in_subquery,
    # )
    if "second_query" in specs:
        second_spec = generate_specifications_for_queries_without_set_ops(
            schema, foreign_keys, specs["second_query"], num
        )
    hash_table = {}

    for _ in range(num):
        set_op_type = random.choice(set_ops_types)
        if set_op_type == "none":
            detail = {
                "set_op_type": set_op_type,
                "first_query": first_spec[random.choice(list(first_spec))],
            }
        else:
            spec1 = first_spec[random.choice(list(first_spec))]
            spec2 = second_spec[random.choice(list(second_spec))]
            if (
                spec1["number_of_value_exp_in_group_by"] != 0
                or spec2["number_of_value_exp_in_group_by"] != 0
            ):
                spec2["number_of_value_exp_in_group_by"] = 0
                spec1["number_of_value_exp_in_group_by"] = 0
                spec1["having_type"] = "none"
                spec2["having_type"] = "none"
            # not *
            spec1["min_max_depth_in_subquery"] = [0, 0]
            spec2["min_max_depth_in_subquery"] = [0, 0]
            spec1["value_exp_types"] = spec2["value_exp_types"]

            detail = {
                "set_op_type": set_op_type,
                "first_query": spec1,
                "second_query": spec2,
            }
        hash_value = calculate_hash(detail)

        if hash_value not in hash_table:
            hash_table[hash_value] = detail
    return hash_table


def generate_specifications_for_queries_without_set_ops(
    schema, foreign_keys, specs, num=100
):
    """
    Generate specifications for queries based on the given schema, primary keys, foreign keys, schema types, and specifications.

    Args:
        schema (dict): The schema dictionary representing the tables and their columns.
        primary_keys (dict): The primary keys dictionary representing the tables and their primary key columns.
        foreign_keys (dict): The foreign keys dictionary representing the tables and their foreign key relationships.
        schema_types (dict): The schema types dictionary representing the tables and their column types.
        specs (dict): The specifications for generating queries.
        num (int, optional): The number of specifications to generate. Defaults to 100.

    Returns:
        dict: The generated specifications as a hash table.
    """
    table_exp_types = specs["table_exp_types"]
    where_clause_types = specs["where_clause_types"]
    number_of_valu_exps_in_group_by = specs["number_of_valu_exps_in_group_by"]
    pattern_matching_types = specs["pattern_matching_types"]
    like_or_not_like = specs["like_or_not_like"]
    basic_comp_ops = specs["basic_comp_ops"]
    null_operators = specs["null_operators"]
    having_types_with_having_group_by = specs["having_types"]
    aggregate_functions_for_having = specs["aggregate_functions_for_having"]
    orderby_types = specs["orderby_types"]
    limit_types = specs["limit_types"]
    value_exp_types = specs["value_exp_types"]
    in_set = specs["in_set"]
    meaningful_joins = specs["meaningful_joins"]
    number_of_value_exps_in_select = specs["number_of_value_exps_in_select"]
    distinct_types = specs["distinct_types"]
    math_func_col_types = specs.get("math_func_col", [])
    string_func_col_types = specs.get("string_func_col", [])
    arithmatic_col_types = specs.get("arithmatic_col", [])
    agg_func_col_types = specs.get("agg_col", [])
    subquery_in_where = specs["subquery_in_where"]
    min_max_depth_in_subquery = specs.get("min_max_depth_in_subquery", [0, 0])
    join_types = specs["join_types"]
    table_exp_types_with_types_of_joins = generate_table_expression_types(
        meaningful_joins, join_types, table_exp_types, schema, foreign_keys
    )

    completed_specifications = {
        "table_exp_types": table_exp_types_with_types_of_joins,
        "where_clause_types": generate_where_clause_types(
            where_clause_types,
            null_operators,
            basic_comp_ops,
            pattern_matching_types,
            like_or_not_like,
            in_set,
            subquery_in_where,
        ),
    }
    if "logical_operators" in where_clause_types:
        add_logical_operator_combinations(
            completed_specifications["where_clause_types"]
        )

    completed_specifications[
        "number_of_value_exps_in_group_by"
    ] = number_of_valu_exps_in_group_by
    completed_specifications[
        "having_types_with_having_group_by"
    ] = generate_having_types(
        having_types_with_having_group_by, aggregate_functions_for_having
    )
    having_types_with_having_group_by = completed_specifications[
        "having_types_with_having_group_by"
    ]

    having_types_without_having_group_by = ["none"]
    all_value_exp_types = generate_all_value_exp_types(
        value_exp_types,
        agg_func_col_types,
        math_func_col_types,
        string_func_col_types,
        arithmatic_col_types,
        number_of_value_exps_in_select,
    )

    return generate_hash_table(
        num,
        table_exp_types_with_types_of_joins,
        completed_specifications["where_clause_types"],
        number_of_valu_exps_in_group_by,
        having_types_without_having_group_by,
        having_types_with_having_group_by,
        orderby_types,
        limit_types,
        meaningful_joins,
        distinct_types,
        all_value_exp_types,
        min_max_depth_in_subquery,
    )


def generate_table_expression_types(
    meaningful_joins, join_types, table_exp_types, schema, foreign_keys
):
    """
    Generate the table expression types for the specifications.

    Args:
        meaningful_joins (str): The flag indicating whether meaningful joins are enabled or not.
        join_types (list): The list of join types.
        table_exp_types (list): The list of table expression types.
        schema (dict): The schema dictionary representing the tables and their columns.
        foreign_keys (dict): The foreign keys dictionary representing the tables and their foreign key relationships.

    Returns:
        list: The generated table expression types with types of joins.
    """
    if meaningful_joins == "yes":
        max_joins, _ = get_max_joins_and_join_definitions(schema, foreign_keys)
        table_exp_types = [
            table_exp_type
            for table_exp_type in table_exp_types
            if not table_exp_type.startswith("join")
            or int(table_exp_type.split("_")[1]) <= max_joins
        ]

    table_exp_types_with_types_of_joins = []
    for table_exp_type in table_exp_types:
        if table_exp_type.startswith("join"):
            _, join_num = table_exp_type.split("_")
            type_of_joins = select_combinations(join_types, int(join_num))
            table_exp_types_with_types_of_joins.extend(
                "_".join(type_of_join) for type_of_join in type_of_joins
            )
        else:
            table_exp_types_with_types_of_joins.append(table_exp_type)
    return table_exp_types_with_types_of_joins


def generate_where_clause_types(
    where_clause_types,
    null_operators,
    basic_comp_ops,
    pattern_matching_types,
    like_or_not_like,
    in_set,
    subquery_in_where,
):
    """
    Generate the where clause types for the specifications.

    Args:
        where_clause_types (list): The list of where clause types.
        null_operators (list): The list of null operators.
        basic_comp_ops (list): The list of basic comparison operators.
        pattern_matching_types (list): The list of pattern matching types.
        like_or_not_like (list): The list of like or not like operators.
        in_set (list): The list of in set types.
        subquery_in_where (list): The list of subquery in where types.

    Returns:
        list: The generated where clause types.
    """
    generated_where_clause_types = []

    for where_type in where_clause_types:
        if where_type in ["none", "between"]:
            generated_where_clause_types.append(where_type)
        elif where_type == "null_check":
            generated_where_clause_types.extend(
                {"null_check": item} for item in null_operators
            )
        elif where_type == "basic_comparison":
            generated_where_clause_types.extend(
                {"basic_comparison": item} for item in basic_comp_ops
            )
        elif where_type == "pattern_matching":
            for like_op in like_or_not_like:
                generated_where_clause_types.extend(
                    {"pattern_matching": [like_op, criteria]}
                    for criteria in pattern_matching_types
                )
        elif where_type == "in_set":
            generated_where_clause_types.extend(in_set)
        elif where_type == "subquery":
            generated_where_clause_types.extend(subquery_in_where)

    return generated_where_clause_types


def add_logical_operator_combinations(where_clause_types):
    """
    Add logical operator combinations to the where clause types.

    Args:
        where_clause_types (list): The list of where clause types.

    Returns:
        None
    """
    max_combination = len(where_clause_types)

    for i in range(max_combination):
        for j in range(i + 1, max_combination):
            if where_clause_types[i] == "none" or where_clause_types[j] == "none":
                continue
            if isinstance(where_clause_types[i], dict) and isinstance(
                where_clause_types[j], dict
            ):
                continue

            first_item = where_clause_types[i]
            second_item = where_clause_types[j]
            if isinstance(where_clause_types[i], dict):
                first_item = list(where_clause_types[i].keys())[0]
            if isinstance(where_clause_types[j], dict):
                second_item = list(where_clause_types[j].keys())[0]

            where_clause_types.append(
                {"logical_operator": ["AND", first_item, second_item]}
            )

            where_clause_types.append(
                {"logical_operator": ["OR", first_item, second_item]}
            )


def generate_having_types(
    having_types_with_having_group_by, aggregate_functions_for_having
):
    """
    Generate the having types for the specifications.

    Args:
        having_types_with_having_group_by (list): The list of having types with having group by.
        aggregate_functions_for_having (list): The list of aggregate functions for having.

    Returns:
        list: The generated having types.
    """
    generated_having_types = []

    if "single" in having_types_with_having_group_by:
        generated_having_types.extend(
            {"single": agg_func} for agg_func in aggregate_functions_for_having
        )
    if "multiple" in having_types_with_having_group_by:
        generated_having_types.append("multiple")

    if "none" in having_types_with_having_group_by:
        generated_having_types.append("none")
    return generated_having_types


def generate_all_value_exp_types(
    value_exp_types,
    agg_func_col_types,
    math_func_col_types,
    string_func_col_types,
    arithmatic_col_types,
    number_of_value_exps_in_select,
):
    """
    Generate all value expression types for the specifications.

    Args:
        value_exp_types (list): The list of value expression types.
        agg_func_col_types (list): The list of aggregate function column types.
        math_func_col_types (list): The list of math function column types.
        string_func_col_types (list): The list of string function column types.
        arithmatic_col_types (list): The list of arithmetic column types.

    Returns:
        list: The generated value expression types.
    """
    all_value_exp_types = []

    if agg_func_col_types:
        if "alias" in agg_func_col_types:
            value_exp_types.append("agg_exp_alias")
        if "no_alias" not in agg_func_col_types and "agg_exp" in value_exp_types:
            value_exp_types.remove("agg_exp")

    if string_func_col_types:
        if "alias" in string_func_col_types:
            value_exp_types.append("string_func_exp_alias")
        if (
            "no_alias" not in string_func_col_types
            and "string_func_exp" in value_exp_types
        ):
            value_exp_types.remove("string_func_exp")

    if arithmatic_col_types:
        if "alias" in arithmatic_col_types:
            value_exp_types.append("arithmatic_exp_alias")
        if (
            "no_alias" not in arithmatic_col_types
            and "arithmatic_exp" in value_exp_types
        ):
            value_exp_types.remove("arithmatic_exp")

    for i in number_of_value_exps_in_select:
        if i == "*":
            all_value_exp_types.append("*")
            continue
        all_combinations = select_combinations(value_exp_types, i)
        all_value_exp_types.extend(all_combinations)

    return all_value_exp_types


def generate_hash_table(
    num,
    table_exp_types_with_types_of_joins,
    where_clause_types,
    number_of_valu_exps_in_group_by,
    having_types_without_having_group_by,
    having_types_with_having_group_by,
    orderby_types,
    limit_types,
    meaningful_joins,
    distinct_types,
    all_value_exp_types,
    min_max_depth_in_subquery,
):
    """
    Generate the hash table of specifications.

    Args:
        num (int): The number of specifications to generate.
        table_exp_types_with_types_of_joins (list): The list of table expression types with types of joins.
        where_clause_types (list): The list of where clause types.
        number_of_valu_exps_in_group_by (list): The list of number of value expressions in group by.
        having_types_without_having_group_by (list): The list of having types without having group by.
        having_types_with_having_group_by (list): The list of having types with having group by.
        orderby_types (list): The list of orderby types.
        limit_types (list): The list of limit types.
        meaningful_joins (list): The list of meaningful joins.
        distinct_types (list): The list of distinct types.
        all_value_exp_types (list): The list of all value expression types.
        min_max_depth_in_subquery (list): The list of min and max depth in subquery.

    Returns:
        dict: The generated hash table of specifications.
    """
    hash_table = {}

    for _ in range(num):
        table_exp_type = random.choice(table_exp_types_with_types_of_joins)
        where_type = random.choice(where_clause_types)
        group_by_type = random.choice(number_of_valu_exps_in_group_by)

        having_type = (
            random.choice(having_types_without_having_group_by)
            if group_by_type == 0
            else random.choice(having_types_with_having_group_by)
        )
        orderby_type = random.choice(orderby_types)
        limit_type = random.choice(limit_types)
        type_of_join = random.choice(meaningful_joins)
        distinct_type = random.choice(distinct_types)
        value_exp_type = random.choice(all_value_exp_types)
        if len(value_exp_type) == 1 and group_by_type == 0:
            orderby_type = random.choice(
                ["ASC", "DESC", "number_ASC", "number_DESC", "none"]
            )  # It cannot be multiple

        detail = {
            "meaningful_joins": type_of_join,
            "table_exp_type": table_exp_type,
            "where_type": where_type,
            "number_of_value_exp_in_group_by": group_by_type,
            "having_type": having_type,
            "orderby_type": orderby_type,
            "limit_type": limit_type,
            "value_exp_types": value_exp_type,
            "distinct_type": distinct_type,
            "min_max_depth_in_subquery": min_max_depth_in_subquery,
        }

        hash_value = calculate_hash(detail)

        if hash_value not in hash_table:
            hash_table[hash_value] = detail

    return hash_table


if __name__ == "__main__":
    schema = all_db["farm"]["schema"]
    pk = all_db["farm"]["primary_keys"]
    fk = all_db["farm"]["foreign_keys"]
    schema_types = all_db["farm"]["schema_types"]
    # change dynamic path to config_file.json

    current_dir = os.path.dirname(__file__)
    dataset_path = os.path.join(current_dir, "../spider/tables.json")
    config_file = os.path.abspath(os.path.join(current_dir, "config_file2.json"))
    # config_file = file_path = os.path.abspath(
    #     "query_generator/query_generation/config_file.json"
    # )
    complete_specs(
        dataset_path,
        config_file,
        db_name="farm",
    )
