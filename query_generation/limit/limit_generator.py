import random


def complete_query_with_limit(temp_query, limit_type):
    if limit_type == "none":
        return temp_query
    limit_clause = generate_limit_clause(limit_type)
    return temp_query + " LIMIT " + limit_clause


def generate_limit_clause(limit_type):
    if limit_type == "without_offset":
        return str(random.randint(1, 10))
    elif limit_type == "with_offset":
        return str(random.randint(1, 10)) + " OFFSET " + str(random.randint(1, 10))
