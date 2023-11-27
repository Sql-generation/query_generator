import random


def generate_limit_clause(limit_type):
    """
    Generate the LIMIT clause based on the given limit_type.

    Args:
        limit_type (str): The type of LIMIT clause to generate.

    Returns:
        str: The generated LIMIT clause.

    Examples:
        >>> generate_limit_clause("without_offset")
        '5'
        >>> generate_limit_clause("with_offset")
        '7 OFFSET 3'
    """
    limit_type_mapping = {
        "without_offset": generate_limit_clause_without_offset,
        "with_offset": generate_limit_clause_with_offset,
    }
    return limit_type_mapping[limit_type]()


def generate_limit_clause_without_offset():
    """
    Generate the LIMIT clause without OFFSET.

    Returns:
        str: The generated LIMIT clause.

    Examples:
        >>> generate_limit_clause_without_offset()
        '5'
    """
    return str(random.randint(1, 10))


def generate_limit_clause_with_offset():
    """
    Generate the LIMIT clause with OFFSET.

    Returns:
        str: The generated LIMIT clause.

    Examples:
        >>> generate_limit_clause_with_offset()
        '7 OFFSET 3'
    """
    return f"{random.randint(1, 10)} OFFSET {random.randint(1, 10)}"
