from neo4j import GraphDatabase
from query_execution import execute_print

# Note in anaconda: add "neo4j+ssc" so that it works
# URI = "neo4j+ssc://1ed43e37.databases.neo4j.io"
URI = "bolt://localhost:7687"

# AUTH = ("neo4j", "PtH0CEI-e3qEhgQ6lHpKBc1eivelLLjCEIvcZwDaaO4")
AUTH = ("neo4j", "123456789")

# Paths to download from Drive (give everyone access)
# # Example: https://drive.google.com/file/d/12C5gyKKYOjVumtx0269CFaYpWCH2juNS/view?usp=drive_link
# Identify the document id ex: "12C5gyKKYOjVumtx0269CFaYpWCH2juNS"
# Download path https://drive.google.com/uc?=download&id="PUT_HERE_ID"

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.verify_connectivity()
        # Clear data
        execute_print(driver, "MATCH (n) DETACH DELETE n")

        # Remove constraints. ATTENTION: APOC needs to be installed to run this procedure
        execute_print(driver, "CALL apoc.schema.assert({}, {})")

        # Create constraints
        execute_print(driver, '''
            CREATE CONSTRAINT Author_name_id IF NOT EXISTS
            FOR (a:Author)
            REQUIRE a.name_id IS UNIQUE
        ''')

        execute_print(driver, '''
            CREATE CONSTRAINT Paper_title IF NOT EXISTS
            FOR (a:Paper) 
            REQUIRE a.title IS UNIQUE
        ''')

        execute_print(driver, '''
            CREATE CONSTRAINT Journal_name IF NOT EXISTS
            FOR (a:Journal) 
            REQUIRE a.name IS UNIQUE
        ''')

        execute_print(driver, '''
            CREATE CONSTRAINT Volume_title IF NOT EXISTS
            FOR (a:Volume)
            REQUIRE a.title IS UNIQUE
        ''')

        execute_print(driver, '''
            CREATE CONSTRAINT Conference_title IF NOT EXISTS
            FOR (a:Conference)
            REQUIRE a.title IS UNIQUE
        ''')

        execute_print(driver, '''
            CREATE CONSTRAINT Edition_title IF NOT EXISTS
            FOR (a:Edition)
            REQUIRE a.title IS UNIQUE
        ''')

        execute_print(driver, '''
            CREATE CONSTRAINT Keyword_tag IF NOT EXISTS
            FOR (a:Keyword)
            REQUIRE a.tag IS UNIQUE
        ''')

        # Load data
        # NODES
        # Author
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///author_node.csv' AS row 
        MERGE (:Author {name_id: row.author_id, name: row.name, surname: row.surname}) 
                            """)

        # Papers
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///paper_node.csv' AS row 
        MERGE (:Paper {title: row.title, DOI: row.DOI, year: toInteger(row.year)
                            , abstract: row.abstract}) 
                            """)

        # Journal
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///journal_node.csv' AS row 
        MERGE (:Journal {title: row.journal, main_editor: row.editor}) 
                            """)

        # Volume
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///volume_node.csv' AS row 
        MERGE (:Volume {title: row.volume, year: toInteger(row.year)})
                            """)

        # Conference
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///conference_node.csv' AS row 
        MERGE (:Conference {title: row.con_shortname})
                            """)

        # Edition
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///edition_node.csv' AS row 
        MERGE (:Edition {title: row.edition_title
            , year: toInteger(row.edition_year)})
                            """)

        # Keyword
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///keywords_node.csv' AS row 
        MERGE (:Keyword {tag: row.keyword})
                            """)

        # EDGES

        # "Writes"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///writes_edge.csv' AS row 
        MATCH (author:Author {name_id: row.main_author})
        MATCH (paper:Paper {title: row.paper})
        MERGE (author)-[:Writes]->(paper)
                            """)
        # "Co_writes"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///co_writes_edge.csv' AS row 
        MATCH (author:Author {name_id: row.co_author})
        MATCH (paper:Paper {title: row.paper})
        MERGE (author)-[:Co_writes]->(paper)
                            """)

        # "From_c"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///from_c_edge.csv' AS row
        MATCH (source:Edition {title: row.edition})
        MATCH (target:Conference {title: row.conference})
        MERGE (source)-[:From_c]->(target)
                            """)
        # "From_j"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///from_j_edge.csv' AS row
        MATCH (v:Volume {title: row.volume})
        MATCH (j:Journal {title: row.journal})
        MERGE (v)-[:From_j]->(j)
                            """)

        # "Published_in_e"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///published_in_e_edge.csv' AS row
        MATCH (source: Paper {title: row.paper })
        MATCH (target: Edition {title: row.edition})
        MERGE (source)-[:Published_in_e]->(target)
                            """)

        # "Published_in_v"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///published_in_v_edge.csv' AS row
        MATCH (source: Paper {title: row.paper })
        MATCH (target: Volume {title: row.volume})
        MERGE (source)-[: Published_in_v]->(target)
                            """)

        # "Reviews"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///reviews_edge.csv' AS row
        MATCH (author:Author {name_id: row.reviewer})
        MATCH (paper:Paper {title: row.paper})
        MERGE (author)-[:Reviews]->(paper)
                            """)

        # "Cites"
        execute_print(driver, """
        LOAD CSV WITH HEADERS FROM 'file:///cites_edge.csv' AS row 
        MATCH (source:Paper {title: row.paper})
        MATCH (target:Paper {title: row.cites})
        MERGE (source)-[:Cites]->(target)
                            """)

        # "From_p"
        execute_print(driver, '''
        LOAD CSV WITH HEADERS FROM 'file:///keywords_node.csv' AS row
        MATCH (k:Keyword {tag: row.keyword})
        MATCH (p:Paper {title: row.paper})
        MERGE (k)-[:From_p]->(p)
                             ''')
    except Exception as e:
        print('\nException raised:')
        print(e)
