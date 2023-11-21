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


# Define a function for generating queries
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
    """

    if not testing_with_one_spec:
        file_name = f"query_generation/output/{db_name}.json"
        with open(file_name) as json_file:
            specs = json.load(json_file)
    else:
        print("Testing with one specification")

        # Load default specs if none provided

        # Load default specs if none provided
        if specs is None:
            # TODO change to read from file
            specs = {
                "farm": {
                    "202bcaa39cf53a2e4d1aaa0d09b4ad7e74b3dbd6": {
                        "meaningful_joins": "mixed",
                        "table_exp_type": "INNER JOIN",
                        "where_type": "IN",
                        "number_of_value_exp_in_group_by": 1,
                        "having_type": {"single": "MAX"},
                        "orderby_type": "ASC",
                        "limit_type": "with_offset",
                        "value_exp_types": ["count_distinct_exp", "arithmatic_exp"],
                        "distinct_type": "none",
                    },
                }
            }
        # Other specifications...

    print("Start generating queries")
    i = 0
    merged_queries = {}

    # Iterate through the specifications for the given db_name
    for hash in specs[db_name]:
        spec = specs[db_name][hash]

        print(spec)
        print(i)
        i += 1
        # spec = list(spec.values())[0]  # Extract values of the dictionary

        # Extract specifications for query components
        table_exp_type = spec["table_exp_type"]
        where_clause_type = spec["where_type"]
        group_by_clause_type = spec["number_of_value_exp_in_group_by"]
        having_type = spec["having_type"]
        order_by_type = spec["orderby_type"]
        limit_type = spec["limit_type"]
        value_exp_types = spec["value_exp_types"]
        meaningful_joins = spec["meaningful_joins"]
        distinct = spec["distinct_type"]
        if is_subquery:
            random_choice = True
        # Generate Table Expressions
        queries_with_attributes = create_table_expression(
            schema,
            pk,
            fk,
            schema_types,
            table_exp_type,
            meaningful_joins,
            random_choice=random_choice,
        )
        random.shuffle(queries_with_attributes)  # Shuffle the queries

        for query_info in queries_with_attributes:
            partial_query, tables, attributes = query_info
            print("************TABLE EXPRESSION ************\n")
            print_attributes(
                partial_query=partial_query, tables=tables, attributes=attributes
            )

            # Complete Where Clause
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
                )
                print("************ WHERE ************")
                for partial_query, attributes in partial_query_with_attributes:
                    print("************ WHERE ************")
                    print_attributes(
                        partial_query=partial_query,
                        tables=tables,
                        attributes=attributes,
                    )

                    # Complete Group By Clause
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
                            # Complete Having Clause
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

                                # Complete SELECT Clause
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

                                        # Complete ORDER BY Clause
                                        partial_query = complete_query_with_order_by(
                                            partial_query,
                                            attributes,
                                            select_clause,
                                            num_value_exps,
                                            order_by_type,
                                        )
                                        print("************ ORDER BY ************")
                                        print_attributes(partial_query=partial_query)

                                        # Complete LIMIT & OFFSET
                                        partial_query = complete_query_with_limit(
                                            partial_query, limit_type
                                        )
                                        print(
                                            "************ LIMIT & OFFSET ************"
                                        )
                                        print_attributes(partial_query=partial_query)

                                        # Prepare query data for output
                                        query_data = {
                                            "Specification": str(spec),
                                            "Partial Query": partial_query,
                                        }

                                        # Check if the specification already exists in merged_queries
                                        if str(spec) in merged_queries:
                                            merged_queries[str(spec)] += (
                                                "\n" + partial_query
                                            )
                                        else:
                                            merged_queries[str(spec)] = partial_query

                                except Exception as e:
                                    print(e)
                                    continue

                    except Exception as e:
                        print(e)
                        continue

            except Exception as e:
                print(e)
                continue

            # Write merged queries to CSV if write_to_csv is True
            if write_to_csv:
                write_queries_to_file(merged_queries=merged_queries)

    print("Done generating queries")
    # print(merged_queries)
    return merged_queries


# File path for schema
file_name = os.path.abspath("SQL_Query_generation/spider/tables.json")
# Read schema information
schema, pk, fk, schema_types = read_schema_pk_fk_types("farm", file_name)

# Generate queries for 'farm' database
query_generator(
    "farm",
    schema,
    pk,
    fk,
    schema_types,
    testing_with_one_spec=True,
    random_choice=False,
)
