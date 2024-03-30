from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

# Run queries
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    # 1. Find the top 3 most cited papers of each conference.
    driver.execute_query('''
        MATCH (conference:Conference)<-[:From]-(edition:Edition)<-[:Is_from]-(cited_p:Paper)<-[citation:Cites]-()
        WITH conference, edition, cited_p, COUNT(citation) as n_citations
        ORDER BY conference, edition, n_citations DESC
        WITH conference, edition, COLLECT(cited_p)[..3] AS topCitedPapers
        UNWIND topCitedPapers AS tCP
        RETURN conference.title AS conference, edition.title as edition, tCP.title as paper
        ORDER BY conference, edition
    ''', database_=db
    )

    # 2. For each conference find its community: i.e., those authors that
    # have published papers on that conference in, at least, 4 different editions.
    driver.execute_query('''
        MATCH (conference:Conference)<-[:From]-(edition:Edition)<-[:Is_from]-(:Paper)-[:Writes|Co_writes]-(author:Author)
        WITH conference.title AS CONFERENCE, author.name_id AS AUTHOR, COUNT(DISTINCT edition) AS EDITIONS
        WHERE EDITIONS >= 4
        RETURN CONFERENCE, AUTHOR
    ''', database_=db
    )