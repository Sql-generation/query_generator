import csv
import json
import os
import random

# Import functions from different modules
from group_by_having import complete_with_group_by_clause
from having import complete_with_having_clause
from helper_funcs import print_attributes, write_queries_to_file
from limit import complete_query_with_limit
from order_by import complete_query_with_order_by
from read_schema import read_schema_pk_fk_types
from select_query import complete_query_with_select
from table_expression import create_table_expression
from where import complete_with_where_clause


def query_generator(
    db_name,
    schema,
    pk,
    fk,
    schema_types,
    specs=None,
    max_num=1000,
    must_be_in_select=None,
    must_be_in_where=None,
    write_to_csv=True,
    is_subquery=False,
    testing_with_one_spec=False,
    random_choice=False,
    types_of_value_exps_for_set_op=None,
):
    """
    Generate queries based on the specifications provided in the specs dictionary.

    Args:
        db_name (str): The name of the database.
        schema (dict): The schema of the database.
        pk (list): The primary key columns.
        fk (list): The foreign key columns.
        schema_types (dict): The data types of the schema.
        specs (dict, optional): The specifications for generating queries. Defaults to None.
        max_num (int, optional): The maximum number of queries to generate. Defaults to 1000.
        must_be_in_select (list, optional): The attributes that must be included in the SELECT clause. Defaults to None.
        must_be_in_where (list, optional): The attributes that must be included in the WHERE clause. Defaults to None.
        write_to_csv (bool, optional): Whether to write the generated queries to a CSV file. Defaults to True.
        is_subquery (bool, optional): Whether the generated queries are subqueries. Defaults to False.
        testing_with_one_spec (bool, optional): Whether to test with one specification. Defaults to False.
        random_choice (bool, optional): Whether to use random choice for certain query components. Defaults to False.

    Returns:
        dict: A dictionary containing the generated queries.
    """
    print("Start reading specifications")

    if not testing_with_one_spec:
        file_name = f"query_generation/output/{db_name}.json"
        with open(file_name) as json_file:
            specs = json.load(json_file)
    else:
        print("Testing with one specification")
        print(specs)
        if specs is None:
            specs = {
                "farm": {
                    # You can replace this dict to test with other specifications in config2.json
                    "dc349a552bd306c584540abb235ee999ac74a8eb": {
                        "set_op_type": "none",
                        "first_query": {
                            "meaningful_joins": "yes",
                            "table_exp_type": "single_table",
                            "where_type": "none",
                            "number_of_value_exp_in_group_by": 1,
                            "having_type": "multiple",
                            "orderby_type": "ASC",
                            "limit_type": "without_offset",
                            "value_exp_types": ["single_exp_text", "string_func_exp"],
                            "distinct_type": "distinct",
                            "min_max_depth_in_subquery": [0, 0],
                        },
                    },
                }
                # "cc20ddc2983eaa4408b28d9917b7191b4672501f": {
                #     "set_op_type": "EXCEPT",
                #     "first_query": {
                #         "meaningful_joins": "no",
                #         "table_exp_type": "INNER JOIN_Join",
                #         "where_type": {
                #             "logical_operator": [
                #                 "AND",
                #                 "NOT IN",
                #                 "pattern_matching",
                #             ]
                #         },
                #         "number_of_value_exp_in_group_by": 0,
                #         "having_type": "none",
                #         "orderby_type": "multiple",
                #         "limit_type": "with_offset",
                #         "value_exp_types": ["agg_exp_alias", "single_exp_number"],
                #         "distinct_type": "distinct",
                #         "min_max_depth_in_subquery": [0, 0],
                #     },
                #     "second_query": {
                #         "meaningful_joins": "yes",
                #         "table_exp_type": "LEFT JOIN_LEFT JOIN_LEFT JOIN",
                #         "where_type": {
                #             "logical_operator": [
                #                 "OR",
                #                 "null_check",
                #                 "exists_subquery",
                #             ]
                #         },
                #         "number_of_value_exp_in_group_by": 0,
                #         "having_type": "none",
                #         "orderby_type": "number_ASC",
                #         "limit_type": "none",
                #         "value_exp_types": ["agg_exp_alias", "single_exp_number"],
                #         "distinct_type": "distinct",
                #         "min_max_depth_in_subquery": [0, 0],
                #     },
                # },
            }

    print("Start generating queries")
    merged_queries = {}

    for i, hash in enumerate(specs[db_name]):
        print(specs[db_name][hash])
        print("************ SET OP ************")
        if "set_op_type" not in specs[db_name][hash]:
            spec = specs[db_name][hash]
        elif specs[db_name][hash]["set_op_type"] == "none":
            spec = specs[db_name][hash]["first_query"]
        else:
            spec = specs[db_name][hash]
            print("************ SET OP ************")

            spec1 = specs[db_name][hash]["first_query"]
            spec2 = specs[db_name][hash]["second_query"]
            first_query = query_generator(
                db_name,
                schema,
                pk,
                fk,
                schema_types,
                specs={db_name: {hash: spec1}},
                write_to_csv=False,
                is_subquery=False,
                testing_with_one_spec=True,
                random_choice=True,
            )

            second_query = query_generator(
                db_name,
                schema,
                pk,
                fk,
                schema_types,
                specs={db_name: {hash: spec2}},
                write_to_csv=False,
                is_subquery=False,
                testing_with_one_spec=True,
                random_choice=True,
            )
            print("_________")
            print(first_query)
            first_query = list(first_query.values())[0].split("\n")[0]
            print("*******")
            print(first_query)
            print("_________")
            print(second_query)
            second_query = list(second_query.values())[0].split("\n")[0]
            completed_query = f"({first_query}) {spec['set_op_type']} ({second_query})"

            if str(spec) in merged_queries:
                merged_queries[str(spec)] += "\n" + completed_query
            else:
                merged_queries[str(spec)] = completed_query
            if write_to_csv:
                write_queries_to_file(merged_queries=merged_queries)

            print("Done generating queries")

            return merged_queries

        table_exp_type = spec["table_exp_type"]
        where_clause_type = spec["where_type"]
        group_by_clause_type = spec["number_of_value_exp_in_group_by"]
        having_type = spec["having_type"]
        order_by_type = spec["orderby_type"]
        limit_type = spec["limit_type"]
        value_exp_types = spec["value_exp_types"]
        meaningful_joins = spec["meaningful_joins"]
        distinct = spec["distinct_type"]
        min_max_depth_in_subquery = spec["min_max_depth_in_subquery"]
        if is_subquery:
            random_choice = True

        queries_with_attributes = create_table_expression(
            schema,
            pk,
            fk,
            schema_types,
            table_exp_type,
            meaningful_joins,
            random_choice=random_choice,
        )
        random.shuffle(queries_with_attributes)

        for query_info in queries_with_attributes:
            partial_query, tables, attributes = query_info
            print("************TABLE EXPRESSION ************\n")
            print_attributes(
                partial_query=partial_query, tables=tables, attributes=attributes
            )

            try:
                partial_query_with_attributes = complete_with_where_clause(
                    schema,
                    schema_types,
                    db_name,
                    partial_query,
                    attributes,
                    where_clause_type,
                    pk,
                    fk,
                    tables,
                    must_be_in_where,
                    random_choice=random_choice,
                    min_max_depth_in_subquery=min_max_depth_in_subquery,
                    query_generator_func=query_generator,
                )
                print("************ WHERE ************")
                for partial_query, attributes in partial_query_with_attributes:
                    print("************ WHERE ************")
                    print_attributes(
                        partial_query=partial_query,
                        tables=tables,
                        attributes=attributes,
                    )

                    try:
                        partial_query_with_attributes = complete_with_group_by_clause(
                            partial_query,
                            attributes,
                            tables,
                            pk,
                            group_by_clause_type,
                            random_choice=random_choice,
                        )
                        print("************ GROUP BY ALL ************")
                        for (
                            partial_query,
                            attributes,
                            must_have_attributes,
                        ) in partial_query_with_attributes:
                            print("************ GROUP BY ************")
                            if must_be_in_select is None:
                                must_be_in_select = []
                            temp = must_be_in_select.copy()
                            for attr in must_have_attributes:
                                temp.append(attr)
                            print_attributes(
                                partial_query=partial_query,
                                attributes=attributes,
                                must_be_in_select=temp,
                            )

                            must_be_in_select1 = temp.copy()

                            partial_query_with_attributes = complete_with_having_clause(
                                partial_query,
                                attributes,
                                must_be_in_select1,
                                having_type,
                                schema,
                                schema_types,
                                db_name,
                                pk,
                                fk,
                                tables,
                                min_max_depth_in_subquery=min_max_depth_in_subquery,
                                query_generator_func=query_generator,
                                random_choice=random_choice,
                            )

                            for (
                                partial_query,
                                attributes,
                                must_be_in_select1,
                            ) in partial_query_with_attributes:
                                print("************ Having ************")
                                print_attributes(
                                    partial_query=partial_query,
                                    attributes=attributes,
                                    must_be_in_select=must_be_in_select1,
                                )

                                try:
                                    partial_query_with_attributes = (
                                        complete_query_with_select(
                                            partial_query,
                                            attributes,
                                            must_be_in_select1,
                                            value_exp_types,
                                            distinct,
                                            is_subquery,
                                            random_choice=random_choice,
                                        )
                                    )
                                    print("************ SELECT ************")
                                    for (
                                        partial_query,
                                        attributes,
                                        must_be_in_select1,
                                        select_clause,
                                        num_value_exps,
                                    ) in partial_query_with_attributes:
                                        print_attributes(
                                            partial_query=partial_query,
                                            attributes=attributes,
                                            must_be_in_select=must_be_in_select1,
                                            select_clause=select_clause,
                                        )

                                        partial_query = complete_query_with_order_by(
                                            partial_query,
                                            attributes,
                                            select_clause,
                                            num_value_exps,
                                            order_by_type,
                                        )
                                        print("************ ORDER BY ************")
                                        print_attributes(partial_query=partial_query)

                                        partial_query = complete_query_with_limit(
                                            partial_query, limit_type
                                        )
                                        print(
                                            "************ LIMIT & OFFSET ************"
                                        )
                                        print_attributes(partial_query=partial_query)

                                        if str(spec) in merged_queries:
                                            merged_queries[str(spec)] += (
                                                "\n" + partial_query
                                            )
                                        else:
                                            merged_queries[str(spec)] = partial_query

                                except Exception as e:
                                    print(e)
                                    if random_choice:
                                        break
                                    else:
                                        continue

                    except Exception as e:
                        print(e)
                        if random_choice:
                            break
                        else:
                            continue

            except Exception as e:
                print(e)
                if random_choice:
                    break
                else:
                    continue

        if write_to_csv:
            write_queries_to_file(merged_queries=merged_queries)

    print("Done generating queries")
    return merged_queries


# File path for schema
current_dir = os.path.dirname(__file__)
file_name = os.path.join(current_dir, "../spider/tables.json")
# Read schema information
schema, pk, fk, schema_types = read_schema_pk_fk_types("farm", file_name)
# print(schema)
# print(pk)
# print(fk)
# print(schema_types)
query_generator(
    "farm",
    schema,
    pk,
    fk,
    schema_types,
    testing_with_one_spec=True,
    random_choice=True,
)
