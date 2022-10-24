from neo4j import GraphDatabase
import json
import csv

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

def create_node(tx, node_label, node_properties):
    command = ""
    for key, value in node_properties.items():
        _key = key.replace(" ", "_")
        command += f"SET n.{_key} = '{value}' "
    tx.run(
        f"CREATE (n:{node_label}) " +
        command
    )

def delete_all(tx):
    tx.run(
        "MATCH (n) "
        "DETACH DELETE n"
    )

def create_relationship(tx, node1_label, node2_label, node1_name, node2_name, reltype):
    tx.run(
        f"MATCH (a:{node1_label}), (b:{node2_label}) "
        f"WHERE a.name = '{node1_name}' AND b.name = '{node2_name}' "
        f"CREATE (a)-[r:{reltype}]->(b)"
    )


if __name__ == "__main__":
    file_number = "0001"
    with open("BP_data_BP-0001 - Sheet1.csv") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        line_count = 0
        nodes = {}
        for row in reader:
            line_count += 1
            if line_count == 1:
                continue
            node_label = row[0]
            property = row[1]
            value = row[2]
            if not node_label in nodes:
                nodes[node_label] = {"name": node_label + file_number}
            nodes[node_label][property] = value
    
    with driver.session() as session:
        session.execute_write(delete_all)
        for node_label, node_properties in nodes.items():
            session.execute_write(create_node, node_label, node_properties)

    with open("relationships.csv") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        line_count = 0
        relationships = []
        for row in reader:
            line_count += 1
            if line_count == 1:
                continue
            relationships.append((row[0], row[1], row[0] + file_number, row[1] + file_number, row[2]))
    with driver.session() as session:
        for rel in relationships:
            session.execute_write(create_relationship, rel[0], rel[1], rel[2], rel[3], rel[4])

    driver.close()