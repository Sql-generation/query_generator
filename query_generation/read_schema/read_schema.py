import json
import os


def update_list_of_lists(lst, c):
    for sub_list in lst:
        if sub_list[0] == c:
            sub_list[1] += 1
            return
    lst.append([c, 1])


def convert_json_to_schema(file_name):
    with open(file_name, "r") as f:
        jason_data = json.load(f)
        all_db = {}
        for db in jason_data:
            all_db[db["db_id"]] = {}
            schema = {}
            primary_keys = {}
            foreign_keys = {}
            schema_types = {}
            for index, table_name in enumerate(db["table_names_original"]):
                schema[table_name] = [
                    column[1]
                    for column in db["column_names_original"]
                    if column[0] == index
                ]

                schema_types[table_name] = {}
                for i in range(len(db["column_names_original"])):
                    if db["column_names_original"][i][0] == index:
                        schema_types[table_name][
                            db["column_names_original"][i][1]
                        ] = db["column_types"][i]
                try:
                    primary_keys[table_name] = db["column_names_original"][
                        db["primary_keys"][index]
                    ][1]
                except:
                    pass
                counting_tables = []
                pairs = []
                if db["foreign_keys"]:
                    for foreign_key in db["foreign_keys"]:
                        local_column_index = foreign_key[0]
                        local_column_index2 = foreign_key[1]

                        table1 = db["table_names_original"][
                            db["column_names_original"][local_column_index][0]
                        ]
                        column1 = db["column_names_original"][local_column_index][1]
                        table2 = db["table_names_original"][
                            db["column_names_original"][local_column_index2][0]
                        ]
                        column2 = db["column_names_original"][local_column_index2][1]
                        update_list_of_lists(counting_tables, table1)
                        update_list_of_lists(counting_tables, table2)
                        pairs.append((table1, column1, table2, column2))
                        sorted_counting_tables = sorted(
                            counting_tables, key=lambda x: x[1], reverse=True
                        )

                    for table in sorted_counting_tables:
                        flag = False

                        foreign_keys[table[0]] = {}
                        for pair in pairs:
                            if pair[0] == table[0]:
                                flag = True
                                foreign_keys[table[0]][pair[1]] = (pair[2], pair[3])
                                pairs.remove(pair)

                            elif pair[2] == table[0]:
                                flag = True
                                foreign_keys[table[0]][pair[3]] = (pair[0], pair[1])
                                pairs.remove(pair)
                        if not flag:
                            foreign_keys.pop(table[0])

            all_db[db["db_id"]]["schema"] = schema
            all_db[db["db_id"]]["primary_keys"] = primary_keys
            all_db[db["db_id"]]["foreign_keys"] = foreign_keys
            all_db[db["db_id"]]["schema_types"] = schema_types
        return all_db


def read_schema_pk_fk_types(db_name, file_name):
    all_db = convert_json_to_schema(file_name)
    schema = all_db[db_name]["schema"]
    pk = all_db[db_name]["primary_keys"]
    fk = all_db[db_name]["foreign_keys"]
    schema_types = all_db[db_name]["schema_types"]
    return schema, pk, fk, schema_types


# all = convert_json_to_schema(
#     {os.path.abspath("SQL_Query_generation/spider/tables.json")}
# )
# print(all["farm"]["schema_types"])
