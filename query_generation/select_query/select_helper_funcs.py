import random

from helper_funcs import generate_arithmetic_expression


def handle_single_expl(select_statement, select_fields, random_column):
    """
    Handles the 'single_expl' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        random_column (str): The random column.

    Returns:
        tuple: The updated select statement and select fields.
    """
    select_statement += f"{random_column}, "
    select_fields.append(random_column)
    return select_statement, select_fields


def handle_alias_exp(select_statement, select_fields, random_column):
    """
    Handles the 'alias_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        random_column (str): The random column.

    Returns:
        tuple: The updated select statement and select fields.
    """
    alias_name = _generate_random_alias_name(select_fields)
    select_statement += f"{random_column} AS {alias_name}, "
    select_fields.append(alias_name)
    return select_statement, select_fields


def handle_arithmatic_exp(
    select_statement, select_fields, col_type, random_column, attributes
):
    """
    Handles the 'arithmatic_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    arithmatic_exp = generate_arithmetic_expression(attributes, num_parts=1)
    if col_type == "arithmatic_exp":
        select_statement += f"{arithmatic_exp}, "
    else:
        alias_name = _generate_random_alias_name(select_fields)
        select_statement += f"{arithmatic_exp} AS {alias_name}, "
        select_fields.append(alias_name)
    return select_statement, select_fields, 1


def handle_string_func_exp(
    select_statement, select_fields, col_type, random_column, attributes
):
    """
    Handles the 'string_func_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    random_string_func = random.choice(_STRING_FUNCTIONS)
    if len(attributes["text"]) == 0:
        raise Exception("No text columns")
    random_column = random.choice(attributes["text"])

    if random_string_func == "SUBSTRING":
        random_column = f"{random_string_func}({random_column}, 1, 3)"
    elif random_string_func == "REPLACE":
        random_column = f"{random_string_func}({random_column}, 'a', 'b')"
    elif random_string_func == "CHARINDEX":
        random_column = f"{random_string_func}('a', {random_column})"
    elif random_string_func == "CONCAT":
        random_column = f"{random_string_func}({random_column}, 'a')"
    elif random_string_func in ["RIGHT", "LEFT"]:
        random_column = f"{random_string_func}({random_column}, 3)"
    else:
        random_column = f"{random_string_func}({random_column})"

    if col_type == "string_func_col_alias":
        alias_name = _generate_random_alias_name(select_fields)
        select_statement += f" AS {alias_name}, "
        select_fields.append(alias_name)

    return select_statement, select_fields, 1


def handle_agg_exp(
    select_statement, select_fields, col_type, random_column, attributes
):
    """
    Handles the 'agg_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    random_agg_func = random.choice(_AGGREGATE_FUNCTIONS)
    random_column = random.choice(attributes["number"])
    # select_fields.append(f"{random_agg_func}({random_column})")

    if col_type != "agg_exp":
        alias_name = _generate_random_alias_name(select_fields)
        select_statement += f" AS {alias_name}, "
        select_fields.append(alias_name)
        random_column = f"{random_agg_func}({random_column}) AS {alias_name}"

    return select_statement, select_fields, 1


def handle_count_distinct_exp(
    select_statement, select_fields, col_type, random_column, attributes
):
    """
    Handles the 'count_distinct_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    random_column = random.choice(attributes["number"] + attributes["text"])
    # select_fields.append(f"COUNT(DISTINCT({random_column}))")

    if col_type != "count_distinct_exp":
        alias_name = _generate_random_alias_name(select_fields)
        select_statement += f" AS {alias_name}, "
        select_fields.append(alias_name)
        random_column = f"COUNT(DISTINCT({random_column})) AS {alias_name}"

    select_fields.append(random_column)
    return select_statement, select_fields, 1


def _generate_random_alias_name(select_fields):
    """
    Generates a random alias name that is not already in the select fields.

    Args:
        select_fields (list): The list of select fields.

    Returns:
        str: The random alias name.
    """
    alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
    while alias_name in select_fields:
        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
    return alias_name


_STRING_FUNCTIONS = [
    "UPPER",
    "LOWER",
    "LENGTH",
    "CONCAT",
    "SUBSTRING",
    "REPLACE",
    "TRIM",
    "LEFT",
    "RIGHT",
    "CHARINDEX",
]

_AGGREGATE_FUNCTIONS = ["MAX", "MIN", "AVG", "SUM", "COUNT"]
