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
                    # You can replace this dict to test with other specifications (uncomment just one of them at a time)
                    # "82df9c03575efeed739b800cafe054795d6c32be": {
                    #     "meaningful_joins": "yes",
                    #     "table_exp_type": "LEFT JOIN_LEFT JOIN",
                    #     "where_type": {
                    #         "logical_operator": [
                    #             "OR",
                    #             "pattern_matching",
                    #             "not_exists_subquery",
                    #         ]
                    #     },
                    #     "number_of_value_exp_in_group_by": 5,
                    #     "having_type": {"single": "COUNT DISTINCT"},
                    #     "orderby_type": "none",
                    #     "limit_type": "with_offset",
                    #     "value_exp_types": ["math_func_exp_alias", "string_func_exp"],
                    #     "distinct_type": "none",
                    #     "min_max_depth_in_subquery": [3, 5],
                    # },
                    # "c7fe5b98614ce3c630dd7238ca6565a9e64ec1e8": {
                    #     "meaningful_joins": "mixed",
                    #     "table_exp_type": "Join_RIGHT JOIN_RIGHT JOIN",
                    #     "where_type": {
                    #         "logical_operator": ["AND", "IN", "pattern_matching"]
                    #     },
                    #     "number_of_value_exp_in_group_by": 4,
                    #     "having_type": {"single": "SUM"},
                    #     "orderby_type": "DESC",
                    #     "limit_type": "none",
                    #     "value_exp_types": ["alias_exp", "arithmatic_exp"],
                    #     "distinct_type": "none",
                    #     "min_max_depth_in_subquery": [3, 5],
                    # }
                    "992ce4c2b42434469200c2af70b2d69bf1bc9213": {
                        "meaningful_joins": "yes",
                        "table_exp_type": "FULL OUTER JOIN_LEFT JOIN_LEFT JOIN",
                        "where_type": {"null_check": "IS NOT NULL"},
                        "number_of_value_exp_in_group_by": 3,
                        "having_type": {"single": "AVG"},
                        "orderby_type": "number_ASC",
                        "limit_type": "with_offset",
                        "value_exp_types": ["agg_exp_alias", "agg_exp_alias"],
                        "distinct_type": "distinct",
                        "min_max_depth_in_subquery": [3, 5],
                    },
                    # "8f3592aea93279425342f18f33ca845dafa2a62e": {
                    #     "meaningful_joins": "no",
                    #     "table_exp_type": "FULL OUTER JOIN_LEFT JOIN",
                    #     "where_type": {
                    #         "logical_operator": [
                    #             "OR",
                    #             "basic_comparison",
                    #             "null_check",
                    #         ]
                    #     },
                    #     "number_of_value_exp_in_group_by": 1,
                    #     "having_type": {"single": "MIN"},
                    #     "orderby_type": "number_ASC",
                    #     "limit_type": "with_offset",
                    #     "value_exp_types": ["count_distinct_exp", "single_exp"],
                    #     "distinct_type": "none",
                    #     "min_max_depth_in_subquery": [3, 5],
                    # },
                    # "347daacee4bf0d202ab585bbd588bbc2f86e2b3c": {
                    #     "meaningful_joins": "yes",
                    #     "table_exp_type": "INNER JOIN_RIGHT JOIN",
                    #     "where_type": {
                    #         "logical_operator": [
                    #             "AND",
                    #             "not_in_with_subquery",
                    #             "comparison_with_subquery",
                    #         ]
                    #     },
                    #     "number_of_value_exp_in_group_by": 0,
                    #     "having_type": "none",
                    #     "orderby_type": "DESC",
                    #     "limit_type": "with_offset",
                    #     "value_exp_types": "*",
                    #     "distinct_type": "distinct",
                    #     "min_max_depth_in_subquery": [3, 5],
                    # },
                }
            }

    print("Start generating queries")
    merged_queries = {}

    for i, hash in enumerate(specs[db_name]):
        spec = specs[db_name][hash]

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

                                        query_data = {
                                            "Specification": str(spec),
                                            "Partial Query": partial_query,
                                        }

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
print(fk)
query_generator(
    "farm",
    schema,
    pk,
    fk,
    schema_types,
    testing_with_one_spec=True,
    random_choice=True,
)
