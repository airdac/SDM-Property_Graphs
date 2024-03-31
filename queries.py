from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

# Run queries
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        # 1. Find the top 3 most cited papers of each conference.
        records, summary, keys = driver.execute_query('''
            MATCH (conference:Conference)<-[:From]-(:Edition)<-[:Is_from]-(cited_p:Paper)<-[citation:Cites]-()
            WITH conference, cited_p, COUNT(citation) as n_citations
            ORDER BY conference, n_citations DESC
            RETURN conference.title AS CONFERENCE, COLLECT(cited_p.title)[..3] AS TOP_CITED_PAPERS
            ORDER BY CONFERENCE
            ''', database_=db
        )
        for record in records:
            print(record)
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, records_count=len(records),
            time=summary.result_available_after
        ))

        # 2. For each conference find its community: i.e., those authors that
        # have published papers on that conference in, at least, 4 different editions.
        records, summary, keys = driver.execute_query('''
            CALL {
                MATCH (conference:Conference)<-[:From]-(edition:Edition)<-[:Is_from]-(:Paper)-[:Writes|Co_writes]-(author:Author)
                WITH conference.title AS CONFERENCE, author.name_id AS AUTHOR, COUNT(DISTINCT edition) AS EDITIONS
                WHERE EDITIONS >= 4
                RETURN CONFERENCE, collect(AUTHOR) AS COMMUNITY
                UNION
                MATCH (conference:Conference)
                RETURN conference.title AS CONFERENCE, null AS COMMUNITY
            }
            RETURN CONFERENCE, COMMUNITY
            ORDER BY COALESCE(size(COMMUNITY), -1) DESC, CONFERENCE
            ''', database_=db
        )
        for record in records:
            print(record)
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, records_count=len(records),
            time=summary.result_available_after
        ))

        # 3. Find the impact factors of the journals in your graph.
        # (see https://en.wikipedia.org/wiki/Impact_factor, for the definition of the impact factor).

        # WE ASSUME THAT A PAPER IS ONLY PUBLISHED ONCE TO SIMPLIFY THE QUERY

        records, summary, keys = driver.execute_query('''
            MATCH (p:Paper)-[:Published_in_v]->(:Volume)-[:From_j]->(j:Journal)
            WHERE p.year IN [date().year-2, date().year-3]
            WITH j.title AS JOURNAL, COUNT(p) AS PUBLICATIONS
            MATCH (citing_p:Paper)-[:Cites]-(cited_p:Paper)-[:Published_in_v]->(:Volume)-[:From_j]->(j:Journal)
            WHERE cited_p.year IN [date().year-2, date().year-3]
                AND citing_p.year = date().year-1
            WITH JOURNAL, PUBLICATIONS, COUNT(citing_p) AS CITATIONS
            RETURN JOURNAL, CITATIONS *1.0 / PUBLICATIONS AS IMPACT_FACTOR
            ORDER BY IMPACT_FACTOR DESC, JOURNAL
            ''', database_=db
        )
        for record in records:
            print(record)
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, records_count=len(records),
            time=summary.result_available_after
        ))

    except Exception as e:
        print(e)