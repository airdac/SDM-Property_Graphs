from neo4j import GraphDatabase

# Note in anaconda: add "neo4j+ssc" so that it works
#URI = "neo4j+ssc://1ed43e37.databases.neo4j.io"
URI = "bolt://localhost:7687"

#AUTH = ("neo4j", "PtH0CEI-e3qEhgQ6lHpKBc1eivelLLjCEIvcZwDaaO4")
#AUTH = ("neo4j", "123456789")
AUTH = ("neo4j", "123456789")


# Paths to download from Drive (give everyone access)
# # Example: https://drive.google.com/file/d/12C5gyKKYOjVumtx0269CFaYpWCH2juNS/view?usp=drive_link
# Identify the document id ex: "12C5gyKKYOjVumtx0269CFaYpWCH2juNS"
# Download path https://drive.google.com/uc?=download&id="PUT_HERE_ID"

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity() 

    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'file:///author_node.csv' AS row 
    WITH row
    WHERE row.title IS NOT NULL
    MERGE (:Paper {title: row.title, DOI: coalesce(row.DOI, "NaN"), month: coalesce(row.month,"NaN"), 
                         abstract: coalesce(row.abstract, "NaN")}) 
                         """)

    print("hi")



