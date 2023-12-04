import random

from helper_funcs import generate_arithmetic_expression

from .select_helper_funcs import (
    _generate_random_alias_name,
    handle_agg_exp,
    handle_alias_exp,
    handle_arithmatic_exp,
    handle_count_distinct_exp,
    handle_single_exp,
    handle_string_func_exp,
)


def complete_query_with_select(
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    is_subquery=False,
    random_choice=False,
):
    """
    Completes the query with the SELECT clause based on the provided parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): The attributes dictionary.
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        distinct_type (str): The distinct type.
        is_subquery (bool, optional): Indicates if it's a subquery. Defaults to False.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: The list of select clauses.
    """

    select_clauses = generate_select_clause(
        temp_query,
        attributes,
        must_have_attributes,
        select_statement_type,
        distinct_type,
        is_subquery=is_subquery,
        random_choice=random_choice,
    )
    print(select_clauses)
    return [
        [
            select_statement + temp_query,
            attributes,
            must_have_attributes,
            select_fields,
            num_value_exps,
        ]
        for select_statement, select_fields, num_value_exps in select_clauses
    ]


def generate_select_clause(
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    has_group_by=None,
    num_columns=None,
    is_subquery=False,
    random_choice=False,
):
    """
    Generates the SELECT clause based on the provided parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): The attributes dictionary.
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        distinct_type (str): The distinct type.
        has_group_by (bool, optional): Indicates if the query has a GROUP BY clause. Defaults to None.
        num_columns (int, optional): The number of columns. Defaults to None.
        is_subquery (bool, optional): Indicates if it's a subquery. Defaults to False.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: The list of select clauses.
    """

    if select_statement_type == "*":
        return [
            [
                "SELECT * ",
                attributes["number"] + attributes["text"],
                len(attributes["number"] + attributes["text"]),
            ]
        ]

    if has_group_by := has_group_by is not False and "GROUP BY" in temp_query:
        return generate_select_clause_with_group_by(
            temp_query,
            attributes,
            must_have_attributes,
            select_statement_type,
            distinct_type,
            has_group_by=False,
            random_choice=random_choice,
        )

    elif is_subquery:
        return generate_select_clause_subquery(
            must_have_attributes,
            select_statement_type,
            attributes,
        )

    else:
        return generate_value_expressions(
            must_have_attributes,
            select_statement_type,
            attributes,
            distinct_type,
            random_choice,
        )


def generate_select_clause_with_group_by(
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    has_group_by=False,
    random_choice=False,
):
    """
    Generates the SELECT clause with GROUP BY based on the provided parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): The attributes dictionary.
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        distinct_type (str): The distinct type.
        has_group_by (bool, optional): Indicates if the query has a GROUP BY clause. Defaults to False.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: The list of select clauses.
    """
    select_statement_with_fields = generate_select_clause(
        temp_query,
        attributes,
        must_have_attributes,
        select_statement_type,
        distinct_type,
        has_group_by=False,
        random_choice=random_choice,
    )
    queries = []
    for temp in select_statement_with_fields:
        select_statement = temp[0]
        select_fields_temp = temp[1]
        select_fields = must_have_attributes.copy()
        num_value_exps = len(select_fields_temp) + len(select_statement_type)
        select_fields += select_fields_temp
        select_statement += ", "
        select_statement += ", ".join(must_have_attributes)

        select_statement = select_statement.removesuffix(", ")
        queries.append([select_statement, select_fields, num_value_exps])

    return queries


def generate_select_clause_subquery(
    must_have_attributes,
    select_statement_type,
    attributes,
):
    """
    Generates the SELECT clause for a subquery based on the provided parameters.

    Args:
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        attributes (dict): The attributes dictionary.

    Returns:
        list: The list of select clauses.
    """
    select_statement = "SELECT " + ", ".join(must_have_attributes)
    select_fields = must_have_attributes.copy()
    num_value_exps = len(select_fields)
    return [[select_statement, select_fields, num_value_exps]]


def generate_value_expressions(
    must_have_attributes,
    select_statement_type,
    attributes,
    distinct_type,
    random_choice,
):
    """
    Generates the SELECT statements based on the provided parameters.

    Args:
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        attributes (dict): The attributes dictionary.
        distinct_type (str): The distinct type.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: The list of select statements.
    """

    select_statements = []  # List to store the generated SELECT statements
    repeat_num = (
        1 if random_choice else 3
    )  # Number of times to repeat the generation process
    for _ in range(repeat_num):
        num_value_exps = 0  # Number of value expressions

        select_statement = "SELECT "
        if distinct_type == "distinct":
            select_statement += (
                "DISTINCT "  # Add DISTINCT keyword if distinct_type is "distinct"
            )
        select_fields = []  # List to store the select fields

        for col_type in select_statement_type:
            num_value_exp = 0  # Number of value expressions
            random_column = random.choice(
                attributes["number"] + attributes["text"]
            )  # Select a random column

            if col_type.startswith("single_exp"):
                number_or_text = col_type.split("_")[2]
                select_statement, select_fields = handle_single_exp(
                    select_statement,
                    select_fields,
                    number_or_text,
                    attributes,
                )

                num_value_exp = 1  # Increment the number of value expressions

            elif col_type == "alias_exp":
                alias_name = _generate_random_alias_name(
                    select_fields
                )  # Generate a random alias name
                select_statement += f"{random_column} AS {alias_name}, "  # Add the column with alias to the SELECT statement
                select_fields.append(
                    alias_name
                )  # Add the alias to the select_fields list
                num_value_exp = 1  # Increment the number of value expressions

            elif col_type.startswith("arithmatic_exp"):
                select_statement, select_fields, num_value_exp = handle_arithmatic_exp(
                    select_statement, select_fields, col_type, random_column, attributes
                )  # Handle arithmetic expression and update select_statement and select_fields

            elif col_type.startswith("string_func_exp"):
                select_statement, select_fields, num_value_exp = handle_string_func_exp(
                    select_statement, select_fields, col_type, random_column, attributes
                )  # Handle string function expression and update select_statement and select_fields

            elif col_type.startswith("agg_exp"):
                select_statement, select_fields, num_value_exp = handle_agg_exp(
                    select_statement, select_fields, col_type, random_column, attributes
                )  # Handle aggregate expression and update select_statement and select_fields

            elif col_type.startswith("count_distinct_exp"):
                (
                    select_statement,
                    select_fields,
                    num_value_exp,
                ) = handle_count_distinct_exp(
                    select_statement, select_fields, col_type, random_column, attributes
                )  # Handle count distinct expression and update select_statement and select_fields
            num_value_exps += num_value_exp  # Increment the number of value expressions
        if select_statement[-2:] == ", ":
            select_statement = select_statement[
                :-2
            ]  # Remove the trailing comma and space
        select_statements.append(
            [select_statement, select_fields, num_value_exps]
        )  # Add the generated select_statement and select_fields to select_statements
    return select_statements
