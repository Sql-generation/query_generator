import itertools
import random

import networkx as nx
from helper_funcs import create_graph_from_schema

# TODO self join


# docstring
def get_max_joins_and_join_definitions(schema, fk):
    """
    Get the maximum number of joins possible and the join definitions.
    """

    join_definitions = create_graph_from_schema(schema, fk)
    max_num_joins = find_max_joins(join_definitions)
    return max_num_joins, join_definitions


def find_max_joins(join_definitions):
    """
    Find the maximum number of joins possible."""
    G = nx.Graph()
    for join in join_definitions:
        table1 = join["table1"]
        table2 = join["table2"]
        G.add_edge(table1, table2)
    return max(len(component) - 1 for component in nx.connected_components(G))


def generate_connections(join_definitions, num_joins):
    """
    Generate connections between tables based on the number of joins."""
    G = nx.Graph()
    for join in join_definitions:
        table1 = join["table1"]
        table2 = join["table2"]
        G.add_edge(table1, table2)

    connections = []
    for combination in itertools.combinations(G.nodes, num_joins):
        subgraph = G.subgraph(combination)
        if nx.is_connected(subgraph):
            connection = []
            for join in join_definitions:
                if join["table1"] in combination and join["table2"] in combination:
                    connection.append(join["table1"])
                    connection.append(join["table2"])
                    connection.append(join["first_key"])
                    connection.append(join["second_key"])
            connections.append(connection)

    return connections


def generate_join_query(schema, fk, join_types, random_choice=False):
    """
    Generate SQL join queries based on the specified number of joins and join types.
    """
    join_definitions = create_graph_from_schema(schema, fk)
    # max_num_joins = find_max_joins(join_definitions)
    connections = generate_connections(join_definitions, int(num_joins) + 1)

    if random_choice:
        connections = [random.choice(connections)]

    queries = []
    for i, connection in enumerate(connections):
        print(i)
        tables = connection[::4]  # Extract table names
        join_conditions = []
        unique_tables = set()  # Use a set to ensure unique table names
        for j in range(len(connection) // 4):
            table1 = connection[j * 4]
            table2 = connection[j * 4 + 1]
            key1 = connection[j * 4 + 2]
            key2 = connection[j * 4 + 3]
            unique_tables.add(table1)
            unique_tables.add(table2)
            join_conditions.append(f"{table1}.{key1} = {table2}.{key2}")
        join_clause = ""
        temp_index = 0
        for table in unique_tables:
            if temp_index == len(join_types):
                join_clause += f"{table}"
            else:
                join_clause += f"{table} {join_types[temp_index]} "
                temp_index += 1

        on_clause = " AND ".join(join_conditions)
        query = f" FROM {join_clause} ON {on_clause}"
        queries.append([query, list(unique_tables)])

    return queries


def generate_meaningless_join(
    schema, num_joins, join_types, num_queries=5, random_choice=False
):
    """
    Generate meaningless SQL join queries based on the specified number of joins and join types.
    """
    if num_joins < 1:
        raise ValueError("The number of joins must be at least 1")

    table_list = list(schema.keys())

    if len(table_list) < num_joins + 1:
        raise ValueError("Not enough tables to perform the requested number of joins")
    temp_queries = []
    if random_choice:
        num_queries = 1
    # Randomly select tables to join
    for i in range(num_queries):
        tables = random.sample(table_list, num_joins + 1)

        join_conditions = []
        unique_tables = set()  # Use a set to ensure unique table names

        for j in range(num_joins):
            table1 = tables[j]
            table2 = tables[j + 1]

            # Randomly select columns from the tables for joining
            key1 = random.choice(schema[table1])
            key2 = random.choice(schema[table2])

            unique_tables.add(table1)
            unique_tables.add(table2)

            join_conditions.append(f"{table1}.{key1} = {table2}.{key2}")

        join_clause = ""
        temp_index = 0
        for table in unique_tables:
            if temp_index == len(join_types):
                join_clause += f"{table}"
            else:
                join_clause += f"{table} {join_types[temp_index]} "
                temp_index += 1
        on_clause = " AND ".join(join_conditions)

        query = f" FROM {join_clause} ON {on_clause}"
        temp_queries.append([query, list(unique_tables)])

    return temp_queries


# Example usage
schema = {
    "table1": ["id", "name", "age"],
    "table2": ["id", "address", "phone"],
    "table3": ["id", "product", "price"],
}
schema_types = {}  # You can add schema types as needed
num_joins = 2
meaningless_join_query = generate_meaningless_join(schema, num_joins, ["INNER JOIN"])
# print(meaningless_join_query)


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
join_definitions = create_graph_from_schema(schema, fk)
# print(generate_join_query(schema, fk, ["INNER JOIN", "INNER JOIN", "INNER JOIN"]))

# Generate and print SQL join queries
# queries = generate_join_query(connections)
# for query in queries:
#     print(query)
# if __name__ == "__main__":
#     print(__package__)
