from neo4j import GraphDatabase
import pandas as pd
from pathlib import Path
from query_execution import execute_print_save

OUT = Path(__file__).parent.parent.absolute()/'B_output'
OUT.mkdir(exist_ok=True)

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

if __name__ == '__main__':
    # Run queries
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        try:
            # 1. Find the top 3 most cited papers of each conference.
            execute_print_save(driver, '''
                MATCH (conference:Conference)<-[:From_c]-(:Edition)<-[:Published_in_e]-(cited_p:Paper)<-[citation:Cites]-()
                WITH conference, cited_p, COUNT(citation) as n_citations
                ORDER BY conference, n_citations DESC
                RETURN conference.title AS CONFERENCE, COLLECT(cited_p.title)[..3] AS TOP_CITED_PAPERS
                ORDER BY CONFERENCE
                ''', OUT/'top3_cited_papers_conf.csv')

            # 2. For each conference find its community: i.e., those authors that
            # have published papers on that conference in, at least, 4 different editions.
            execute_print_save(driver, '''
                CALL {
                    MATCH (conference:Conference)<-[:From_c]-(edition:Edition)<-[:Published_in_e]-(:Paper)-[:Writes|Co_writes]-(author:Author)
                    WITH conference.title AS CONFERENCE, author.name_id AS AUTHOR, COUNT(DISTINCT edition) AS EDITIONS
                    WHERE EDITIONS >= 4
                    RETURN CONFERENCE, collect(AUTHOR) AS COMMUNITY
                    UNION
                    MATCH (conference:Conference)
                    RETURN conference.title AS CONFERENCE, null AS COMMUNITY
                }
                RETURN CONFERENCE, COMMUNITY
                ORDER BY COALESCE(size(COMMUNITY), -1) DESC, CONFERENCE
                ''', OUT/'communities.csv')

            # 3. Find the impact factors of the journals in your graph.
            # (see https://en.wikipedia.org/wiki/Impact_factor, for the definition of the impact factor).

            # WE ASSUME THAT A PAPER IS ONLY PUBLISHED ONCE (that is no volumes or editions have any paper in common) TO SIMPLIFY THE QUERY

            execute_print_save(driver, '''
                MATCH (p:Paper)-[:Published_in_v]->(:Volume)-[:From_j]->(j:Journal)
                WHERE p.year IN [date().year-2, date().year-3]
                WITH j.title AS JOURNAL, COUNT(p) AS PUBLICATIONS
                MATCH (citing_p:Paper)-[:Cites]-(cited_p:Paper)-[:Published_in_v]->(:Volume)-[:From_j]->(j:Journal)
                WHERE cited_p.year IN [date().year-2, date().year-3]
                    AND citing_p.year = date().year-1
                WITH JOURNAL, PUBLICATIONS, COUNT(citing_p) AS CITATIONS
                RETURN JOURNAL, CITATIONS *1.0 / PUBLICATIONS AS IMPACT_FACTOR
                ORDER BY IMPACT_FACTOR DESC, JOURNAL
                ''', OUT/'impact_factors.csv')

            # 4. Find the h-indexes of the authors in your graph
            # (see https://en.wikipedia.org/wiki/H-index, for a definition of the h-index metric)

            # WE ASSUME THAT A PAPER IS ONLY PUBLISHED ONCE (that is no volumes or editions have any paper in common) TO SIMPLIFY THE QUERY

            execute_print_save(driver, '''
                MATCH (a:Author)-[:Writes|Co_writes]->(p:Paper)<-[:Cites]-(:Paper)
                WITH a.name_id AS author, p.title as p_title, COUNT(*) AS c
                ORDER BY author, c DESC
                WITH author, collect({paper:p_title, citations:c}) AS paperCitations, COUNT(p_title) AS n_papers
                UNWIND range(0, n_papers-1) AS row_number
                WITH author, paperCitations[row_number]['paper'] as paper, paperCitations[row_number]['citations'] AS citations, row_number
                WHERE citations >= row_number+1
                RETURN author, MAX(row_number)+1 AS h_index
                ORDER BY author
                ''', OUT/'h-indexes.csv')

        except Exception as e:
            print('\nException raised:')
            print(e)
