import random


def complete_with_having_clause(
    temp_query,
    attributes,
    must_have_attributes,
    having_clauses_types,
    random_choice=False,
):
    if having_clauses_types == "none":
        return [[temp_query, attributes, must_have_attributes]]
    elif having_clauses_types == "subquery":
        pass
    elif having_clauses_types == "multiple":
        logical_ops = ["AND", "OR"]
        aggregate_functions = ["MAX", "MIN", "AVG", "SUM", "COUNT", "COUNT DISTINCT"]

        random_logical_op = random.choice(logical_ops)
        random_agg_func = random.sample(aggregate_functions, 2)

        having_clause1 = create_having_clause(
            attributes, random_agg_func[0], random_choice=True
        )

        having_clause2 = create_having_clause(
            attributes, random_agg_func[1], random_choice=True
        )
        return [
            [
                f"{temp_query} HAVING (({having_clause1}) {random_logical_op} ({having_clause2}))",
                attributes,
                must_have_attributes,
            ]
        ]
    if having_clauses_types["single"]:
        having_clauses = create_having_clause(
            attributes, having_clauses_types["single"], random_choice=random_choice
        )
        queries = []
        for having_clause in having_clauses:
            queries.append(
                [
                    f"{temp_query} HAVING {having_clause}",
                    attributes,
                    must_have_attributes,
                ]
            )

        return queries


def create_having_clause(attributes, aggregate_function, random_choice=False):
    ops = ["=", ">", "<", ">=", "<="]
    having_clauses = []
    if random_choice:
        ops = [random.choice(ops)]
    for op in ops:
        if aggregate_function == "COUNT DISTINCT":
            random_column = random.choice(attributes["number"] + attributes["text"])
            having_clause = (
                f"COUNT(DISTINCT({random_column})) {op} {random.randint(1, 100)}"
            )
            having_clauses.append(having_clause)
        else:
            random_column = random.choice(attributes["number"])
            having_clause = (
                f"{aggregate_function}({random_column}) {op} {random.randint(1, 100)}"
            )
            having_clauses.append(having_clause)
    return having_clauses

    # elif having_state == "multiple":
    #     ops = ["=", ">", "<", ">=", "<="]
    #     and_or = ["AND", "OR"]
    #     for op in ops:
    #         for logical_op in and_or:
    #             random_column_with_aggregate2 = random.choice(
    #                 attributes["number"]
    #             )
    #             while (
    #                 random_column_with_aggregate2
    #                 == random_column_with_aggregate
    #                 or random_column_with_aggregate2
    #                 == random_column
    #             ):
    #                 random_column_with_aggregate2 = (
    #                     random.choice(attributes["number"])
    #                 )
    #             aggregate_function2 = random.choice(
    #                 aggregate_functions
    #             )
    #             having_clause = f"(({aggregate_function}({random_column_with_aggregate}) {op} {random.randint(1, 100)}) {logical_op} ({aggregate_function2}({random_column_with_aggregate2}) {op} {random.randint(1, 100)}))"
    #             group_by_query = f"{temp_query} GROUP BY {', '.join(random_columns)} HAVING {having_clause}"
    #             select_clause = f"{random_column}, {aggregate_function}({random_column_with_aggregate}), {aggregate_function2}({random_column_with_aggregate2})"
