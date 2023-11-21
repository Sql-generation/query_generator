import random

from helper_funcs import generate_arithmetic_expression


def complete_query_with_select(
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    is_subquery=False,
    random_choice=False,
):
    select_clauses = generate_select_clause(
        temp_query,
        attributes,
        must_have_attributes,
        select_statement_type,
        distinct_type,
        is_subquery=is_subquery,
        random_choice=random_choice,
    )
    queries = []
    for select_clause, select_fields, num_value_exps in select_clauses:
        queries.append(
            [
                select_clause + temp_query,
                attributes,
                must_have_attributes,
                select_fields,
                num_value_exps,
            ]
        )
    return queries


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
    if has_group_by is not False and "GROUP BY" in temp_query:
        has_group_by = True
    else:
        has_group_by = False
    if select_statement_type == "*":
        return ["SELECT * "]
    if num_columns is None:
        num_col_in_select = len(select_statement_type)
    else:
        num_col_in_select = num_columns

    temp_queries = []
    select_clause = ""
    temp_select_clause = ", ".join(must_have_attributes)
    all_text_number_columns = attributes["number"] + attributes["text"]
    select_fields = []
    num_value_exps = 0
    if has_group_by:
        select_clause_with_fields = generate_select_clause(
            temp_query,
            attributes,
            must_have_attributes,
            select_statement_type,
            distinct_type,
            has_group_by=False,
            random_choice=random_choice,
        )
        queries = []

        select_clause = ""
        temp_select_clause = ", ".join(must_have_attributes)
        all_text_number_columns = attributes["number"] + attributes["text"]
        select_fields = []
        for temp in select_clause_with_fields:
            select_clause = temp[0]
            select_fields_temp = temp[1]
            select_fields = must_have_attributes.copy()
            num_value_exps = len(select_fields_temp) + num_col_in_select
            select_fields += select_fields_temp

            select_clause += ", "
            select_clause += temp_select_clause
            if select_clause[-2:] == ", ":
                select_clause = select_clause[:-2]
            queries.append([select_clause, select_fields, num_value_exps])
        return queries
    elif is_subquery:
        select_clause = "SELECT "
        select_clause += ", ".join(must_have_attributes)
        select_fields = must_have_attributes.copy()
        num_value_exps = len(select_fields)
        return [[select_clause, select_fields, num_value_exps]]

    else:
        select_clauses = []
        repeat_num = 3
        if random_choice:
            repeat_num = 1
        for _ in range(repeat_num):
            select_clause = "SELECT "
            if "distinct" == distinct_type:
                select_clause += "DISTINCT "
            select_fields = []

            for col_type in select_statement_type:
                random_column = random.choice(all_text_number_columns)

                if col_type == "single_expl":
                    select_clause += random_column + ", "
                    select_fields.append(random_column)
                elif col_type == "alias_exp":
                    alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                    while alias_name in select_fields:
                        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                    select_clause += random_column + " AS " + alias_name + ", "
                    select_fields.append(alias_name)
                elif col_type.startswith("arithmatic_exp"):
                    arithmatic_exp = generate_arithmetic_expression(
                        attributes, num_parts=1
                    )
                    if col_type == "arithmatic_exp":
                        select_clause += arithmatic_exp + ", "
                    else:
                        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        while alias_name in select_fields:
                            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        select_clause += arithmatic_exp + " AS " + alias_name + ", "
                        select_fields.append(alias_name)

                elif col_type.startswith("string_func_exp"):
                    string_funcs = [
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
                    random_string_func = random.choice(string_funcs)
                    if len(attributes["text"]) == 0:
                        raise Exception("No text columns")
                    random_column = random.choice(attributes["text"])
                    if random_string_func == "SUBSTRING":
                        select_clause += (
                            random_string_func + "(" + random_column + ", 1, 3), "
                        )
                    elif random_string_func == "REPLACE":
                        select_clause += (
                            random_string_func + "(" + random_column + ", 'a', 'b'), "
                        )
                    elif random_string_func == "CHARINDEX":
                        select_clause += (
                            random_string_func + "('a', " + random_column + "), "
                        )
                    elif random_string_func == "CONCAT":
                        select_clause += (
                            random_string_func + "(" + random_column + ", 'a'), "
                        )
                    elif random_string_func == "RIGHT" or random_string_func == "LEFT":
                        select_clause += (
                            random_string_func + "(" + random_column + ", 3), "
                        )
                    else:
                        select_clause += (
                            random_string_func + "(" + random_column + "), "
                        )
                    if col_type == "string_func_col_alias":
                        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")

                        while alias_name in select_fields:
                            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        select_clause += " AS " + alias_name + ", "
                        select_fields.append(alias_name)
                elif col_type.startswith("agg_exp"):
                    aggregate_functions = ["MAX", "MIN", "AVG", "SUM", "COUNT"]
                    random_agg_func = random.choice(aggregate_functions)
                    random_column = random.choice(attributes["number"])
                    select_clause += random_agg_func + "(" + random_column + "), "

                    if col_type != "agg_exp":
                        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        while alias_name in select_fields:
                            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        select_clause += " AS " + alias_name + ", "
                        select_fields.append(alias_name)
                elif col_type.startswith("count_distinct_exp"):
                    random_column = random.choice(all_text_number_columns)
                    select_clause += "COUNT(DISTINCT(" + random_column + ")), "
                    if col_type != "count_distinct_exp":
                        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        while alias_name in select_fields:
                            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
                        select_clause += " AS " + alias_name + ", "
                        select_fields.append(alias_name)

            if select_clause[-2:] == ", ":
                select_clause = select_clause[:-2]
            select_clauses.append([select_clause, select_fields])

    return select_clauses


temp_query = (
    " FROM farm JOIN competition_record ON competition_record.farm_id = farm.farm_id WHERE  farm.cows >= 76 GROUP BY competition_record.competition_id HAVING MAX(farm.pigs) = 3",
    ["farm", "competition_record"],
    {
        "number": [
            "farm.farm_id",
            "farm.year",
            "farm.total_horses",
            "farm.working_horses",
            "farm.total_cattle",
            "farm.oxen",
            "farm.bulls",
            "farm.cows",
            "farm.pigs",
            "farm.sheep_and_goats",
            "competition_record.competition_id",
            "competition_record.farm_id",
            "competition_record.rank",
        ],
        "text": [],
    },
    "competition_record.competition_id, MAX(farm.pigs)",
)
details = [
    ["distinct", "no_distinct", "count_func", "count_distinct"],
    [1, 2, 3, 4, "*"],
]
# complete_query_with_select(temp_query, details)
