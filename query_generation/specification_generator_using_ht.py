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
    all_db = convert_json_to_schema(db_file)
    specs = {}

    with open(config_file, "r") as f:
        spec_config = json.load(f)

    if db_name:
        print(db_name)

        specs[db_name] = generate_specifications_for_queries(
            all_db[db_name]["schema"],
            all_db[db_name]["primary_keys"],
            all_db[db_name]["foreign_keys"],
            all_db[db_name]["schema_types"],
            spec_config,
        )
        # shuffle specs
        # random.shuffle(specs[db_name])

        current_dir = os.path.dirname(__file__)
        file_name = os.path.join(current_dir, f"output/{db_name}.json")
        write_hash_table_to_json(
            specs,
            file_name,
        )

    else:
        for db in all_db:
            print(db)
            # print(specs)
            specs[db] = generate_specifications_for_queries(
                all_db[db]["schema"],
                all_db[db]["primary_keys"],
                all_db[db]["foreign_keys"],
                all_db[db]["schema_types"],
                spec_config,
            )

            current_dir = os.path.dirname(__file__)
            file_name_for_write = os.path.join(current_dir, f"output/{db}.json")
            print(file_name_for_write)
            write_hash_table_to_json(specs[db], file_name_for_write)


def generate_specifications_for_queries(
    schema, primary_keys, foreign_keys, schema_types, specs, num=100
):
    completed_specifications = {}
    details = []
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
    # value_expression_types = specs["value_expression_types"]
    value_exp_types = specs["value_exp_types"]
    in_with_subquery = specs["in_with_subquery"]
    in_set = specs["in_set"]
    # logical_combinations = specs["logical_combinations"]
    meaningful_joins = specs["meaningful_joins"]
    number_of_value_exps_in_select = specs["number_of_value_exps_in_select"]
    distinct_types = specs["distinct_types"]
    math_func_col_types = []
    string_func_col_types = []
    arithmatic_col_types = []
    agg_func_col_types = []
    subquery_in_where = specs["subquery_in_where"]
    join_types = specs["join_types"]
    if "math_func_col" in specs:
        math_func_col_types = specs["math_func_col"]
    if "string_func_col" in specs:
        string_func_col_types = specs["string_func_col"]

    if "arithmatic_col" in specs:
        arithmatic_col_types = specs["arithmatic_col"]
    if "agg_col" in specs:
        agg_func_col_types = specs["agg_col"]

    if specs["meaningful_joins"] == "yes":
        max_joins, _ = get_max_joins_and_join_definitions(schema, foreign_keys)
        for table_exp_type in table_exp_types:
            if table_exp_type.startswith("join"):
                table_exp_type, join_num = table_exp_type.split("_")
                if int(join_num) > max_joins:
                    table_exp_types.remove(table_exp_type)

    table_exp_types_with_types_of_joins = []
    for table_exp_type in table_exp_types:
        if table_exp_type.startswith("join"):
            _, join_num = table_exp_type.split("_")
            # table_exp_types.remove(table_exp_type)
            type_of_joins = select_combinations(join_types, int(join_num))
            for type_of_join in type_of_joins:
                temp = "_".join(type_of_join)
                table_exp_types_with_types_of_joins.append(temp)
        else:
            table_exp_types_with_types_of_joins.append(table_exp_type)
    # print(table_exp_types_with_types_of_joins)
    completed_specifications["table_exp_types"] = table_exp_types_with_types_of_joins

    completed_specifications["where_clause_types"] = []
    for where_type in where_clause_types:
        if where_type == "none" or where_type == "between":
            completed_specifications["where_clause_types"].append(where_type)
        elif where_type == "null_check":
            for item in null_operators:
                completed_specifications["where_clause_types"].append(
                    {"null_check": item}
                )
        elif where_type == "basic_comparison":
            for item in basic_comp_ops:
                completed_specifications["where_clause_types"].append(
                    {"basic_comparison": item}
                )
        elif where_type == "pattern_matching":
            for like_op in like_or_not_like:
                for criteria in pattern_matching_types:
                    completed_specifications["where_clause_types"].append(
                        {
                            "pattern_matching": [
                                like_op,
                                criteria,
                            ]
                        }
                    )

        elif where_type == "in_set":
            for item in in_set:
                completed_specifications["where_clause_types"].append(item)

        elif where_type == "subquery":
            for item in subquery_in_where:
                if item == "in_with_subquery":
                    for in_type in in_with_subquery:
                        if in_type == "in":
                            completed_specifications["where_clause_types"].append(item)
                        else:
                            completed_specifications["where_clause_types"].append(
                                "not_in_with_subquery"
                            )
                else:
                    completed_specifications["where_clause_types"].append(item)

    if "logical_operators" in where_clause_types:
        max_combination = len(completed_specifications["where_clause_types"])
        for i in range(max_combination):
            for j in range(i + 1, max_combination):
                if (
                    completed_specifications["where_clause_types"][i] == "none"
                    or completed_specifications["where_clause_types"][j] == "none"
                ):
                    continue
                if isinstance(
                    completed_specifications["where_clause_types"][i], dict
                ) and isinstance(
                    completed_specifications["where_clause_types"][j], dict
                ):
                    continue

                first_item = completed_specifications["where_clause_types"][i]
                second_item = completed_specifications["where_clause_types"][j]
                if isinstance(completed_specifications["where_clause_types"][i], dict):
                    first_item = list(
                        completed_specifications["where_clause_types"][i].keys()
                    )[0]
                if isinstance(completed_specifications["where_clause_types"][j], dict):
                    second_item = list(
                        completed_specifications["where_clause_types"][j].keys()
                    )[0]

                completed_specifications["where_clause_types"].append(
                    {
                        "logical_operator": [
                            "AND",
                            first_item,
                            second_item,
                        ]
                    }
                )

                completed_specifications["where_clause_types"].append(
                    {
                        "logical_operator": [
                            "OR",
                            first_item,
                            second_item,
                        ]
                    }
                )

    completed_specifications["number_of_value_exps_in_group_by"] = []
    for i in number_of_valu_exps_in_group_by:
        completed_specifications["number_of_value_exps_in_group_by"].append(i)
    completed_specifications["having_types_with_having_group_by"] = []
    # TODO op
    if "single" in having_types_with_having_group_by:
        for agg_func in aggregate_functions_for_having:
            completed_specifications["having_types_with_having_group_by"].append(
                {"single": agg_func}
            )
    if "multiple" in having_types_with_having_group_by:
        completed_specifications["having_types_with_having_group_by"].append("multiple")
    if "none" in having_types_with_having_group_by:
        completed_specifications["having_types_with_having_group_by"].append("none")

    having_types_with_having_group_by = completed_specifications[
        "having_types_with_having_group_by"
    ]
    having_types_without_having_group_by = ["none"]
    all_value_exp_types = []
    if agg_func_col_types:
        if "alias" in agg_func_col_types:
            value_exp_types.append("agg_exp_alias")
        if "no_alias" not in agg_func_col_types:
            if "agg_exp" in value_exp_types:
                value_exp_types.remove("agg_exp")
    if math_func_col_types:
        if "alias" in math_func_col_types:
            value_exp_types.append("math_func_exp_alias")
        if "no_alias" not in math_func_col_types:
            if "math_func_exp" in value_exp_types:
                value_exp_types.remove("math_func_exp")
    if string_func_col_types:
        if "alias" in string_func_col_types:
            value_exp_types.append("string_func_exp_alias")
        if "no_alias" not in string_func_col_types:
            if "string_func_exp" in value_exp_types:
                value_exp_types.remove("string_func_exp")
    if arithmatic_col_types:
        if "alias" in arithmatic_col_types:
            value_exp_types.append("arithmatic_exp_alias")
        if "no_alias" not in arithmatic_col_types:
            if "arithmatic_exp" in value_exp_types:
                value_exp_types.remove("arithmatic_exp")
    # print(select_statement_types)
    for i in number_of_value_exps_in_select:
        if i == "*":
            all_value_exp_types.append("*")
            continue
        all_combinations = select_combinations(value_exp_types, i)
        for combination in all_combinations:
            all_value_exp_types.append(combination)
    hash_table = {}

    for i in range(num):
        table_exp_type = random.choice(table_exp_types_with_types_of_joins)
        where_type = random.choice(completed_specifications["where_clause_types"])
        group_by_type = random.choice(number_of_valu_exps_in_group_by)
        if group_by_type == 0:
            having_type = random.choice(having_types_without_having_group_by)
        else:
            having_type = random.choice(having_types_with_having_group_by)
            value_exp_types2 = []
            if "agg_exp" in specs["value_exp_types"]:
                value_exp_types2.append("agg_exp")
            if "count_distinct_exp" in specs["value_exp_types"]:
                value_exp_types2.append("count_distinct_exp")
                selcet_types2 = []

            for i in number_of_value_exps_in_select:
                if i == "*":
                    continue
                all_combinations = select_combinations(value_exp_types2, i)
                for combination in all_combinations:
                    selcet_types2.append(combination)
            all_value_exp_types = selcet_types2
        orderby_type = random.choice(orderby_types)
        limit_type = random.choice(limit_types)
        type_of_join = random.choice(meaningful_joins)
        distinct_type = random.choice(distinct_types)
        value_exp_type = random.choice(all_value_exp_types)
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
    config_file = os.path.abspath(os.path.join(current_dir, "config_file.json"))
    # config_file = file_path = os.path.abspath(
    #     "query_generator/query_generation/config_file.json"
    # )
    complete_specs(
        dataset_path,
        config_file,
        db_name="farm",
    )
