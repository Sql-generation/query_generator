import random


def complete_query_with_order_by(
    temp_query, attributes, select_clause, num_value_exps, order_by_type
):
    if order_by_type == "none":
        return temp_query
    order_by_clause = generate_order_by_clause(
        attributes, select_clause, num_value_exps, order_by_type
    )
    return temp_query + " ORDER BY " + order_by_clause


def generate_order_by_clause(attributes, select_clause, num_value_exps, order_by_type):
    if "*" in select_clause:
        select_clause = attributes["number"] + attributes["text"]
    if order_by_type == "multiple":
        order_by_clause = ""
        random_num = random.randint(1, len(select_clause) // 2)
        random_attributes = random.sample(select_clause, random_num)

        for i in range(len(random_attributes)):
            random_order = random.choice(["ASC", "DESC"])
            order_by_clause += f"{random_attributes[i]} {random_order}, "
        return order_by_clause[:-2]
    elif order_by_type == "ASC" or "DESC":
        random_attribute = random.choice(select_clause)
        return f"{random_attribute} {order_by_type}"
    elif order_by_type == "number_ASC" or "number_DESC":
        random_value_exp = random.randint(1, num_value_exps)
        return f"{random_value_exp} {order_by_type.split('_')[1]}"
