import logging
import os
import pandas as pd
from py2neo import Graph, Node, Relationship, NodeMatcher
import time
import numpy as np
import ast

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def create_paper_node(data, tx):
    for idx, data in data.iterrows():
        try:
            arxiv_id = data["arxiv_id"]
            title = data["title"]
            abstract = data["abstract"]
            authors = ast.literal_eval(data["authors"])
            categories = data["categories"]
            created = data["created"]
            doi = data["doi"]
            primary_category = data["primary_category"]
            updated = data["updated"]
            # Creating Paper node
            paper_node = Node("paper", arxiv_id=arxiv_id, title=title, abstract=abstract, authors=authors,
                              categories=categories, created=created, doi=doi, primary_category=primary_category, updated=updated)
            tx.create(paper_node)
            print("Create Paper Node")
            # Create each node for author of a paper
            for author in authors:
                author_node = Node("author", author=author)
                tx.create(author_node)
            print("Created Author Node")

        except Exception as identifier:
            print(identifier)
            pass
    tx.commit()


def create_author_relationship(data, graph):
    matcher = NodeMatcher(graph)
    for idx, data in data.iterrows():
        try:
            arxiv_id = data["arxiv_id"]
            authors = ast.literal_eval(data["authors"])

            for author in authors:
                existing_papers = matcher.match(
                    'paper', arxiv_id=arxiv_id).first()

                existing_authors = matcher.match(
                    'author', author=author).first()
                print(existing_authors)
                relationship_wtitten_by = Relationship.type("WRITTEN_BY")
                graph.merge(relationship_wtitten_by(
                    existing_papers, existing_authors), "paper", "author")
                print("Made relationship between", arxiv_id, "and", author)
        except Exception as identifier:
            print(identifier)
            pass


if __name__ == "__main__":

    graph = Graph()
    data = pd.read_csv("../data/2017-01-01full.csv")
    data = data.replace(np.nan, '', regex=True)
    print(data.head(3))
    tx = graph.begin()
    create_paper_node(data.head(1000), tx)
    create_author_relationship(data.head(1000), graph)
