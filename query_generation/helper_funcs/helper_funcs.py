import csv
import hashlib
import itertools

#     return colms
import json
import os
import random


def get_attributes_ends_with(name, attributes):
    attrs = attributes["number"] + attributes["text"]
    for attribute in attrs:
        if attribute.endswith(name):
            return attribute
    return None


def get_corresponding_fk_table(table_of_pk, join_definitions):
    possible_tables_and_pk = []
    for pairs in join_definitions:
        if pairs["table1"] == table_of_pk:
            possible_tables_and_pk.append([pairs["table2"], pairs["second_key"]])
        elif pairs["table2"] == table_of_pk:
            possible_tables_and_pk.append([pairs["table1"], pairs["first_key"]])
    return possible_tables_and_pk


def write_queries_to_file(merged_queries):
    current_dir = os.path.dirname(__file__)
    output_dir = os.path.abspath(os.path.join(current_dir, "../output"))
    csv_file = os.path.join(output_dir, "res.csv")
    with open(csv_file, mode="w", newline="") as csv_file:
        fieldnames = ["Specification", "Partial Query"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for spec, merged_query in merged_queries.items():
            query_data = {
                "Specification": spec,
                "Partial Query": merged_query,
            }
            writer.writerow(query_data)


def get_table_name_from_column(column, schema):
    tables = []
    col = column
    if column.find(".") != -1:
        tables.append(column.split(".")[0])
        col = column.split(".")[1]

    for table in schema:
        if col in schema[table]:
            if table not in tables:
                tables.append(table)
    return tables


def generate_arithmetic_expression(attributes, num_parts=None, max_depth=3):
    if num_parts is None:
        num_parts = random.randint(1, 2)
        # num_parts = random.randint(1, len(attributes["number"]) // 2)

    part_of_arithmetic_expression_types = [
        "constant",
        "column",
        "arithmetic_expression_with_par",
    ]
    arithmetic_expression_types = ["+", "-", "*", "/"]
    math_funcs = [
        "ABS",
        "CEILING",
        "FLOOR",
        "ROUND",
        "EXP",
        "POWER",
        "SQRT",
        "LOG",
        "LOG10",
        "RAND",
        "SIGN",
    ]

    arithmetic_expression = []
    numeric_attributes = attributes["number"]
    for i in range(num_parts):
        if max_depth <= 0:
            break  #
        random_part_of_arithmetic_expression = random.choice(
            part_of_arithmetic_expression_types
        )

        if random_part_of_arithmetic_expression == "constant":
            random_constant = random.randint(1, 100)
            arithmetic_expression.append(str(random_constant))
            if i != num_parts - 1:
                random_arithmetic_expression = random.choice(
                    arithmetic_expression_types
                )
                arithmetic_expression.append(random_arithmetic_expression)

        elif random_part_of_arithmetic_expression == "column":
            random_column = random.choice(attributes["number"])
            if random.choice([True, False]):
                arithmetic_expression.append(random_column)

                if i != num_parts - 1:
                    random_arithmetic_expression = random.choice(
                        arithmetic_expression_types
                    )
                    arithmetic_expression.append(random_arithmetic_expression)

            else:
                if random.choice([True, False]):
                    random_math_func = random.choice(math_funcs)
                    if random_math_func == "POWER" or random_math_func == "ROUND":
                        arithmetic_expression.append(
                            f"{random_math_func}({random_column},{random.randint(2,5)})"
                        )
                    else:
                        arithmetic_expression.append(
                            f"{random_math_func}({random_column})"
                        )
                    if i != num_parts - 1:
                        random_arithmetic_expression = random.choice(
                            arithmetic_expression_types
                        )
                        arithmetic_expression.append(random_arithmetic_expression)
                else:
                    random_number_of_parts = random.randint(1, 3)
                    random_arithmetic_expression = generate_arithmetic_expression(
                        attributes,
                        num_parts=random_number_of_parts,
                        max_depth=max_depth - 1,
                    )
                    random_math_func = random.choice(math_funcs)
                    if random_math_func == "POWER" or random_math_func == "ROUND":
                        arithmetic_expression.append(
                            f"{random_math_func}({random_arithmetic_expression},{random.randint(2,5)})"
                        )
                    else:
                        arithmetic_expression.append(
                            f"{random_math_func}({random_arithmetic_expression})"
                        )

                    if i != num_parts - 1:
                        random_arithmetic_expression = random.choice(
                            arithmetic_expression_types
                        )
                        arithmetic_expression.append(random_arithmetic_expression)

        elif random_part_of_arithmetic_expression == "arithmetic_expression_with_par":
            random_number_of_parts = random.randint(2, 3)
            random_arithmetic_expression = generate_arithmetic_expression(
                attributes, num_parts=random_number_of_parts, max_depth=max_depth - 1
            )
            arithmetic_expression.append(f"({random_arithmetic_expression})")
            if i != num_parts - 1:
                random_arithmetic_expression = random.choice(
                    arithmetic_expression_types
                )
                arithmetic_expression.append(random_arithmetic_expression)

    return " ".join(arithmetic_expression)


def write_hash_table_to_json(hash_table, file_name):
    json_object = json.dumps(hash_table, indent=4)
    # Writing to sample.json
    with open(file_name, "w") as outfile:
        outfile.write(json_object)
        return


def calculate_hash(d):
    json_str = json.dumps(d, sort_keys=True)

    # Generate a hash of the JSON string
    hash_object = hashlib.sha1(json_str.encode())
    hash_hex = hash_object.hexdigest()

    return hash_hex


def write_detail_to_json(details, file_name):
    json_object = json.dumps(details, indent=4)
    # Writing to sample.json
    with open(file_name, "w") as outfile:
        outfile.write(json_object)
        return


def create_graph_from_schema(schema, fk):
    graph = []
    for table in fk:
        fk_for_tables = list(fk[table].keys())
        i = 0
        for table2_with_key in fk[table].values():
            graph.append(
                (
                    {
                        "table1": table,
                        "table2": table2_with_key[0],
                        "first_key": fk_for_tables[i],
                        "second_key": table2_with_key[1],
                    }
                )
            )

            i += 1
    return graph


def all_colms(schema, schema_type, unique_tables, alias=None):
    columns = {"number": [], "text": []}
    column_names = {}  # Dictionary to store column names and their corresponding tables
    if len(unique_tables) == 1:
        table = unique_tables[0]
        for col_name in schema[table]:
            if schema_type[table][col_name] == "number":
                if alias:
                    columns["number"].append(f"{alias[0]}.{col_name}")
                else:
                    columns["number"].append(col_name)
            elif schema_type[table][col_name] == "text":
                if alias:
                    columns["text"].append(f"{alias[0]}.{col_name}")
                else:
                    columns["text"].append(col_name)
        return columns
    else:
        # Collect column names and corresponding tables
        for table in unique_tables:
            for col_name in schema[table]:
                full_col_name = f"{table}.{col_name}"
                if col_name not in column_names:
                    column_names[col_name] = [full_col_name]
                else:
                    column_names[col_name].append(full_col_name)

        # Iterate through shared column names and create formatted pairs
        for col_name, tables in column_names.items():
            if len(tables) > 1:
                formatted_cols = tables
                if schema_type[tables[0].split(".")[0]][col_name] == "number":
                    columns["number"].extend(formatted_cols)
                else:
                    columns["text"].extend(formatted_cols)
            else:
                table, col_name = tables[0].split(".")
                if schema_type[table][col_name] == "number":
                    columns["number"].append(col_name)
                elif schema_type[table][col_name] == "text":
                    columns["text"].append(col_name)
        return columns


def random_not_pk_cols(attributes, unique_tables, pk, number_of_col):
    all_cols = attributes["number"] + attributes["text"]
    if isinstance(unique_tables, list):
        for table in unique_tables:
            if pk[table] in all_cols:
                all_cols.remove(pk[table])
            if table + "." + pk[table] in all_cols:
                all_cols.remove(table + "." + pk[table])
    elif isinstance(unique_tables, str):
        if pk[unique_tables] in all_cols:
            all_cols.remove(pk[unique_tables])

    else:
        for alias_table in unique_tables:
            if alias_table + "." + pk[unique_tables[alias_table]] in all_cols:
                all_cols.remove(alias_table + "." + pk[unique_tables[alias_table]])
    if len(all_cols) < number_of_col:
        raise Exception("Sample larger than population")
    sample_sets = []
    for _ in range(2):
        sample_set = random.sample(all_cols, number_of_col)
        while sample_set in sample_sets:
            sample_set = random.sample(all_cols, number_of_col)

        sample_sets.append(sample_set)

    return sample_sets


# Generate combinations for the specified number of columns
def select_combinations(select_statement_types, num_columns):
    combinations = []

    for combination in itertools.product(select_statement_types, repeat=num_columns):
        combinations.append(combination)

    unique_combinations = set()

    for combination in combinations:
        # Sort the combination to make it consistent and eliminate duplicates
        sorted_combination = tuple(sorted(combination))
        unique_combinations.add(sorted_combination)

    # Convert the unique_combinations set back to a list of tuples
    select_combinations_list = [
        list(combination) for combination in unique_combinations
    ]

    return select_combinations_list


def print_attributes(**kwargs):
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    print()


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
foreign_keys = {
    "farm_competition": {"host_city_id": ("city", "city_id")},
    "competition_record": {
        "farm_id": ("farm", "farm_id"),
        "competition_id": ("farm_competition", "competition_id"),
    },
}
fk = {
    "farm_competition": {"host_city_id": ("city", "city_id")},
    "competition_record": {
        "farm_id": ("farm", "farm_id"),
        "competition_id": ("farm_competition", "competition_id"),
    },
}
temp_queries = "FROM city JOIN competition_record JOIN farm JOIN farm_competition ON farm_competition.host_city_id = city.city_id AND competition_record.farm_id = farm.farm_id AND competition_record.competition_id = farm_competition.competition_id"
# all_columns = all_colms(schema, schema_types, ["city"], ["m"])
# print(all_columns)
# print(create_graph_from_schema(schema, fk))

# # Example usage:
# attributes = {"number": ["col1", "col2", "col3"]}
# expression = generate_arithmetic_expression(attributes, 1)
