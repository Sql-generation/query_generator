import random


def generate_order_by_clause(attributes, select_clause, num_value_exps, order_by_type):
    """
    Generate the ORDER BY clause based on the given attributes, select_clause, num_value_exps, and order_by_type.

    Args:
        attributes (dict): A dictionary containing the attributes.
        select_clause (list): The list of attributes in the select clause.
        num_value_exps (int): The number of value expressions.
        order_by_type (str): The type of ORDER BY clause to generate.

    Returns:
        str: The generated ORDER BY clause.

    Examples:
        >>> generate_order_by_clause({"number": ["col1"], "text": ["col2"]}, ["col1", "col2"], 5, "multiple")
        'col1 ASC, col2 DESC'
        >>> generate_order_by_clause({"number": ["col1"], "text": ["col2"]}, ["col1", "col2"], 5, "ASC")
        'col1 ASC'
        >>> generate_order_by_clause({"number": ["col1"], "text": ["col2"]}, ["col1", "col2"], 5, "number_DESC")
        '3 DESC'
    """

    # If select_clause contains "*", replace it with the combined attributes
    if "*" in select_clause:
        select_clause = attributes["number"] + attributes["text"]

    # Mapping of order_by_type to corresponding generation logic
    order_by_type_mapping = {
        "multiple": generate_order_by_clause_multiple,
        "ASC": generate_order_by_clause_single,
        "DESC": generate_order_by_clause_single,
        "number_ASC": generate_order_by_clause_number,
        "number_DESC": generate_order_by_clause_number,
    }

    # Generate the ORDER BY clause based on the order_by_type
    return order_by_type_mapping[order_by_type](select_clause, num_value_exps)


def generate_order_by_clause_multiple(select_clause, num_value_exps):
    """
    Generate the ORDER BY clause for the "multiple" order_by_type.

    Args:
        select_clause (list): The list of attributes in the select clause.
        num_value_exps (int): The number of value expressions.

    Returns:
        str: The generated ORDER BY clause.

    Examples:
        >>> generate_order_by_clause_multiple(["col1", "col2"], 5)
        'col1 ASC, col2 DESC'
    """
    random_num = random.randint(1, len(select_clause) // 2)
    random_attributes = random.sample(select_clause, random_num)
    return ", ".join(
        f"{attr} {random.choice(['ASC', 'DESC'])}" for attr in random_attributes
    )


def generate_order_by_clause_single(select_clause, num_value_exps):
    """
    Generate the ORDER BY clause for the "ASC" or "DESC" order_by_type.

    Args:
        select_clause (list): The list of attributes in the select clause.
        num_value_exps (int): The number of value expressions.

    Returns:
        str: The generated ORDER BY clause.

    Examples:
        >>> generate_order_by_clause_single(["col1", "col2"], 5)
        'col1 ASC'
    """
    random_attribute = random.choice(select_clause)
    return f"{random_attribute} {order_by_type}"


def generate_order_by_clause_number(select_clause, num_value_exps):
    """
    Generate the ORDER BY clause for the "number_ASC" or "number_DESC" order_by_type.

    Args:
        select_clause (list): The list of attributes in the select clause.
        num_value_exps (int): The number of value expressions.

    Returns:
        str: The generated ORDER BY clause.

    Examples:
        >>> generate_order_by_clause_number(["col1", "col2"], 5)
        '3 DESC'
    """
    random_value_exp = random.randint(1, num_value_exps)
    return f"{random_value_exp} {order_by_type.split('_')[1]}"
