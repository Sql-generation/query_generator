import random

from helper_funcs import random_not_pk_cols


def complete_with_group_by_clause(
    temp_query, attributes, unique_tables, pk, number_of_col, random_choice=False
):
    select_clause = ""
    must_have_attrs = []

    if number_of_col == 0:
        return [[temp_query, attributes, must_have_attrs]]
    try:
        sample_stes = random_not_pk_cols(attributes, unique_tables, pk, number_of_col)
        print("sample_stes", sample_stes)
        queries = []
        if random_choice:
            sample_stes = [random.choice(sample_stes)]
        for random_columns in sample_stes:
            group_by_query = f"{temp_query} GROUP BY "
            must_have_attrs = random_columns
            group_by_query += ", ".join(random_columns)
            queries.append([group_by_query, attributes, must_have_attrs])
        return queries
    except:
        raise Exception("Error in group by clause")


# Example usage:
temp_queries_with_columns = [
    "FROM farm JOIN competition_record ON competition_record.farm_id = farm.farm_id WHERE  farm.cows >= 76",
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
    # Add more queries here
]
having_details = ["single", "multiple"]

details = {"single": ["MAX"], "multiple": ["MIN", "AVG"]}

# result = complete_query_with_group_by_and_having(
#     temp_queries_with_columns, details, having_details
# )

# for query, unique_tables, all_columns, select_clause in result:
#     print("Query:", query)
#     print("Unique Tables:", unique_tables)
#     print("All Columns:", all_columns)
#     print("Select Clause:", select_clause)
#     print()
