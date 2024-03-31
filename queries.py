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

        # for each journal:
        #   count(citations (in volumes of journals of current year -1 or editions of conferences of current year - 1)
        #       of papers published in (volumes of current year - 2 or current year - 3))
        #   /
        #   count(papers published in volumes of journal of (current year -2 or current year - 3))
        
        # Steps:
        #   0. RETURN current year
        #       RETURN date().year
        #   0.1 RETURN current year - 2
        #       RETURN date().year - 2
        #   1. COUNT(papers published in volumes of journal of current year -2)
        records, summary, keys = driver.execute_query('''
            
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