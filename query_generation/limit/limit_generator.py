import random


def generate_limit_clause(limit_type):
    limit_type_mapping = {
        "without_offset": generate_limit_clause_without_offset,
        "with_offset": generate_limit_clause_with_offset,
    }
    return limit_type_mapping[limit_type]()


def generate_limit_clause_without_offset():
    return str(random.randint(1, 10))


def generate_limit_clause_with_offset():
    return f"{random.randint(1, 10)} OFFSET {random.randint(1, 10)}"
