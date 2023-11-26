import json
import random
import string

from helper_funcs import generate_arithmetic_expression, get_attributes_ends_with

from .subquery import generate_subquery


def all_colms(schema, schema_type, unique_tables):
    colms = {"number": [], "text": []}
    for table in unique_tables:
        for colm in schema[table]:
            if schema_type[table][colm] == "number":
                colms["number"].append(table + "." + colm)
            elif schema_type[table][colm] == "text":
                colms["text"].append(table + "." + colm)
    return colms


def generate_random_words(word_list, num_words):
    return random.sample(word_list, num_words)


def generate_like_pattern(criteria):
    # TODO change it
    if criteria == "starts_with_a":
        return "a%"
    elif criteria == "ends_with_ing":
        return "%ing"
    elif criteria == "contains_apple":
        return "%apple%"
    elif criteria == "exactly_5_characters":
        return "_____"
    elif criteria == "ends_with_at":
        return "%at%"
    elif criteria == "does_not_contain_xyz":
        return "%[^xyz]%"
    elif criteria == "starts_with_A_or_B":
        return "[AB]%"
    elif criteria == "ends_with_ing_or_ed":
        return "%(ing|ed)"
    elif criteria == "alphanumeric":
        return "%[A-Za-z0-9]%"
    elif criteria == "starts_with_vowel":
        return "[AEIOU]%"

    return None


def create_where_clause(
    schema, schema_types, db_name, colms, details, pk, fk, tables, random_choice=False
):
    print(details)
    if "none" in details:
        return ""
    if "basic_comparison" in details:
        if colms["number"]:
            random_numeric_colm = random.choice(colms["number"])
            if "basic_comparison" == details:
                where_clauses = []
                comparison_operators = ["=", "<>", "!=", ">", "<", ">=", "<="]
                if random_choice:
                    comparison_operator = random.choice(comparison_operators)
                    random_exp = generate_arithmetic_expression(colms)

                    where_cluase = " {} {} {}".format(
                        random_numeric_colm, comparison_operator, random_exp
                    )
                    return [where_cluase]
                else:
                    for comparison_operator in comparison_operators:
                        #
                        random_exp = generate_arithmetic_expression(colms)

                        where_cluase = " {} {} {}".format(
                            random_numeric_colm, comparison_operator, random_exp
                        )
                        where_clauses.append(where_cluase)

                    return where_clauses

            comparison_operator = details["basic_comparison"]
            # TODO change random_number
            random_exp = generate_arithmetic_expression(colms)

            where_cluase = " {} {} {}".format(
                random_numeric_colm, comparison_operator, random_exp
            )

            return [where_cluase]
        else:
            raise Exception("There is no number column in the schema")
    if "pattern_matching" in details:
        if colms["text"]:
            random_text_colm = random.choice(colms["text"])
            if details == "pattern_matching":
                where_clauses = []
                pattern_matching_types = [
                    "starts_with_a",
                    "ends_with_ing",
                    "exactly_5_characters",
                    "does_not_contain_xyz",
                ]
                if random_choice:
                    pattern_type = random.choice(pattern_matching_types)
                    pattern = generate_like_pattern(pattern_type)
                    if pattern:
                        if random.choice([True, False]):
                            where_cluase = "{} LIKE {}".format(
                                random_text_colm, pattern
                            )
                        else:
                            where_cluase = "{} NOT LIKE {}".format(
                                random_text_colm, pattern
                            )
                        return [where_cluase]
                else:
                    for pattern_type in pattern_matching_types:
                        pattern = generate_like_pattern(pattern_type)
                        if pattern:
                            where_cluase = "{} LIKE {}".format(
                                random_text_colm, pattern
                            )

                            where_clauses.append(where_cluase)
                            where_cluase = "{} NOT LIKE {}".format(
                                random_text_colm, pattern
                            )
                            where_clauses.append(where_cluase)

                    print(where_clauses)
                    return where_clauses

            if isinstance(details["pattern_matching"], list):
                op = details["pattern_matching"][0]
                pattern_type = details["pattern_matching"][1]
                pattern = generate_like_pattern(pattern_type)
                if pattern:
                    where_cluase = "{} {} {}".format(random_text_colm, op, pattern)
                    print(where_cluase)
                    return [where_cluase]

        else:
            raise Exception("There is no text column in the schema")

    if "null_check" in details:
        random_text_colm = random.choice(colms["text"] + colms["number"])
        if "null_check" == details:
            where_clauses = []
            null_check_operators = ["IS NULL", "IS NOT NULL"]
            if random_choice:
                op = random.choice(null_check_operators)
                where_cluase = "{} {}".format(random_text_colm, op)
                return [where_cluase]
            else:
                for op in null_check_operators:
                    where_cluase = "{} {}".format(random_text_colm, op)
                    where_clauses.append(where_cluase)
                return where_clauses
        op = details["null_check"][0]
        where_cluase = "{} {}".format(random_text_colm, op)
        return [where_cluase]
    if "IN" in details:
        if len(colms["text"]) == 0 and len(colms["number"]) == 0:
            if random.choice([True, False]):
                random_text_colm = random.choice(colms["text"])
                word_list = ["apple", "banana", "orange", "grape", "pineapple"]
                random_words = generate_random_words(word_list, 5)

                where_cluase = "{} IN ({})".format(
                    random_text_colm, ", ".join(random_words)
                )
                return [where_cluase]
            else:
                random_numeric_colm = random.choice(colms["number"])
                list_of_numbers = ["1", "2", "3", "40", "5"]

                where_cluase = "{} IN ({})".format(
                    random_numeric_colm, ", ".join(list_of_numbers)
                )
                return where_cluase
        elif colms["text"]:
            random_text_colm = random.choice(colms["text"])
            word_list = ["apple", "banana", "orange", "grape", "pineapple"]
            random_words = generate_random_words(word_list, 5)

            where_cluase = "{} IN ({})".format(
                random_text_colm, ", ".join(random_words)
            )
            return [where_cluase]
        elif colms["number"]:
            random_numeric_colm = random.choice(colms["number"])
            list_of_numbers = ["1", "2", "3", "40", "5"]

            where_cluase = "{} IN ({})".format(
                random_numeric_colm, ", ".join(list_of_numbers)
            )
            return [where_cluase]
    if "NOT IN" in details:
        if len(colms["text"]) == 0 and len(colms["number"]) == 0:
            if random.choice([True, False]):
                random_text_colm = random.choice(colms["text"])
                word_list = ["apple", "banana", "orange", "grape", "pineapple"]
                random_words = generate_random_words(word_list, 5)

                where_cluase = "{} NOT IN ({})".format(
                    random_text_colm, ", ".join(random_words)
                )
                return [where_cluase]
            else:
                random_numeric_colm = random.choice(colms["number"])
                list_of_numbers = ["1", "2", "3", "40", "5"]

                where_cluase = "{}NOT IN ({})".format(
                    random_numeric_colm, ", ".join(list_of_numbers)
                )
                return [where_cluase]
        elif colms["text"]:
            random_text_colm = random.choice(colms["text"])
            word_list = ["apple", "banana", "orange", "grape", "pineapple"]
            random_words = generate_random_words(word_list, 5)

            where_cluase = "{} IN ({})".format(
                random_text_colm, ", ".join(random_words)
            )
            return [where_cluase]
        elif colms["number"]:
            random_numeric_colm = random.choice(colms["number"])
            list_of_numbers = ["1", "2", "3", "40", "5"]

            where_cluase = "{} IN ({})".format(
                random_numeric_colm, ", ".join(list_of_numbers)
            )
            return [where_cluase]

    if "between" in details:
        if colms["number"]:
            random_numeric_colm = random.choice(colms["number"])
            where_cluase = "{} BETWEEN {} AND {}".format(random_numeric_colm, 1, 10)
            return [where_cluase]
        else:
            raise Exception("There is no number column in the schema")
    if "logical_operator" in details:
        # TODO
        predicator1 = create_where_clause(
            schema,
            schema_types,
            db_name,
            colms,
            details,
            ["logical_operator"][1],
            pk,
            fk,
            tables,
            random_choice=random_choice,
        )
        op = details["logical_operator"][0]
        predicator2 = create_where_clause(
            schema,
            schema_types,
            db_name,
            colms,
            details["logical_operator"][2],
            pk,
            fk,
            tables,
            random_choice=random_choice,
        )
        where_clauses = []
        for first_predicator in predicator1:
            for second_predicator in predicator2:
                where_cluase = "({}) {} ({})".format(
                    first_predicator, op, second_predicator
                )
                where_clauses.append(where_cluase)
    if "subquery" in details:
        print("subquery")
        where_cluase = generate_subquery(
            schema, schema_types, db_name, colms, details, pk, fk, tables
        )

        return where_cluase


def complete_with_where_clause(
    schema,
    schema_types,
    db_name,
    temp_query,
    attributes,
    where_clauses_types,
    pk,
    fk,
    tables,
    must_be_in_where=None,
    random_choice=False,
):
    try:
        where_cluase = create_where_clause(
            schema,
            schema_types,
            db_name,
            attributes,
            where_clauses_types,
            pk,
            fk,
            tables,
            random_choice=random_choice,
        )

        if where_cluase == "":
            if must_be_in_where:
                return [
                    [
                        temp_query
                        + " WHERE "
                        + must_be_in_where[0]
                        + get_attributes_ends_with(must_be_in_where[1], attributes),
                        attributes,
                    ]
                ]
            return [[temp_query, attributes]]
        if isinstance(where_cluase, list):
            if not must_be_in_where:
                queries = []
                for where in where_cluase:
                    queries.append([temp_query + " WHERE " + where, attributes])
            else:
                queries = []
                for where in where_cluase:
                    queries.append(
                        [
                            temp_query
                            + " WHERE "
                            + where
                            + " AND "
                            + must_be_in_where[0]
                            + get_attributes_ends_with(must_be_in_where[1], attributes),
                            attributes,
                        ]
                    )
        return queries
    except Exception as e:
        raise e


schema = {
    "city": [
        "city_id",
        "official_name",
        "status",
        "area_km_2",
        "population",
        "census_ranking",
    ],
    "farm": [
        "farm_id",
        "year",
        "total_horses",
        "working_horses",
        "total_cattle",
        "oxen",
        "bulls",
        "cows",
        "pigs",
        "sheep_and_goats",
    ],
    "farm_competition": ["competition_id", "year", "theme", "host_city_id", "hosts"],
    "competition_record": ["competition_id", "farm_id", "rank"],
}
schema_types = {
    "city": {
        "city_id": "number",
        "official_name": "text",
        "status": "text",
        "area_km_2": "number",
        "population": "number",
        "census_ranking": "text",
    },
    "farm": {
        "farm_id": "number",
        "year": "number",
        "total_horses": "number",
        "working_horses": "number",
        "total_cattle": "number",
        "oxen": "number",
        "bulls": "number",
        "cows": "number",
        "pigs": "number",
        "sheep_and_goats": "number",
    },
    "farm_competition": {
        "competition_id": "number",
        "year": "number",
        "theme": "text",
        "host_city_id": "number",
        "hosts": "text",
    },
    "competition_record": {
        "competition_id": "number",
        "farm_id": "number",
        "rank": "number",
    },
}

temp_query = "FROM city JOIN competition_record JOIN farm JOIN farm_competition ON farm_competition.host_city_id = city.city_id AND competition_record.farm_id = farm.farm_id AND competition_record.competition_id = farm_competition.competition_id"

# all_columns = all_colms(
#     schema,
#     schema_types,
#     ["city", "competition_record", "farm", "farm_competition"],
# )
# create_where_clause(
#     all_columns,
#     [
#         "basic_comparison",
#         "pattern_matching",
#         "null_check",
#         "in",
#         "between",
#         "logical_operators",
#     ],
# )
# complete_queries(
#     temp_query,
#     ["city", "competition_record", "farm", "farm_competition"],
#     schema,
#     schema_types,
# )
