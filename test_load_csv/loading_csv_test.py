from neo4j import GraphDatabase

# Note: i need to add "neo4j+ssc" so that it works
URI = "neo4j+ssc://bad73995.databases.neo4j.io"
AUTH = ("neo4j", "shqApK5RyOU6FXip3liHb6s5CgEaO_NR0o8dQTqyJ8Q")

# Good practices:
# Add the database in all operations
# Create indexes if you need to look for some value always
# Download a file from drive
# 1) Get the full sharing link
# Example: https://drive.google.com/file/d/12C5gyKKYOjVumtx0269CFaYpWCH2juNS/view?usp=drive_link
# Identify the document id ex: "12C5gyKKYOjVumtx0269CFaYpWCH2juNS"
# Download path https://drive.google.com/uc?export="PUT_HERE_ID"
# Result: https://drive.google.com/uc?export=download&id=12C5gyKKYOjVumtx0269CFaYpWCH2juNS

#Note: Add """Query"""" to write as much as we want

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    driver.execute_query("""
    LOAD CSV FROM 'https://drive.google.com/uc?export=download&id=12C5gyKKYOjVumtx0269CFaYpWCH2juNS' AS row 
    MERGE (a:Artist {name: row[1], year: toInteger(row[2])}) RETURN a.name, a.year
                               """)

    driver.close()
