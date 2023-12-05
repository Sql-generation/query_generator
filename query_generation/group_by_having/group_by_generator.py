import random

from helper_funcs import random_not_pk_cols


def complete_with_group_by_clause(
    temp_query, attributes, unique_tables, pk, number_of_col, random_choice=False
):
    """
    Complete the query with the GROUP BY clause based on the given parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): A dictionary containing the attributes.
        unique_tables (list): The list of unique tables.
        pk (str): The primary key.
        number_of_col (int): The number of columns to include in the GROUP BY clause.
        random_choice (bool, optional): Whether to use random choice or return all possible queries. Defaults to False.

    Returns:
        list: A list of completed queries with the GROUP BY clause and attributes and value expressions that must be in select statement.

    Raises:
        Exception: If there is an error in the GROUP BY clause.

    Examples:
        >>> complete_with_group_by_clause("FROM table", {"number": ["col1", "col2"], "text": ["col3"]}, ["table"], "col1", 2)
        [['FROM table GROUP BY col1, col2', {'number': ['col1', 'col2'], 'text': ['col3']}, ['col1', 'col2']]]
    """

    if number_of_col == 0:
        return [
            [temp_query, attributes, []]
        ]  # Return the original query with an empty list for must_be_in_select_statement

    try:
        print("ioioio")
        print(unique_tables)
        sample_stes = random_not_pk_cols(
            attributes, unique_tables, pk, number_of_col
        )  # Generate random column combinations
        print("sample_stes", sample_stes)
        queries = []
        if random_choice:
            sample_stes = [
                random.choice(sample_stes)
            ]  # Select a random combination if random_choice is True

        for random_columns in sample_stes:
            group_by_query = f"{temp_query} GROUP BY {', '.join(random_columns)}"  # Construct the GROUP BY query
            queries.append(
                [group_by_query, attributes, random_columns]
            )  # Append the query, attributes, and random columns to the list that must be in select statement

        return queries

    except Exception as e:
        raise Exception(
            "Error in GROUP BY clause"
        ) from e  # Raise an exception if there is an error in the GROUP BY clause
