We start by creating a recommender for the database community, given by the keywords:
- data management
- indexing
- data modeling
- big data
- data processing
- data storage
- data querying
### Finding papers from the database community
 There are no papers with any of the previous keywords assigned. Hence, let us assign them randomly by adding them to the set of possible keywords.
# Following the steps to create a recommender
## 1.  Define the database community
```cypher
MERGE (r:Research_community {name: 'database'})
```
```cypher
MATCH (k:Keyword)
WHERE k.tag IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
MATCH (r:Research_community)
MERGE (k)-[:From_r]->(r)
```
## 2. Find conferences and journals related to the database community
Nº of papers per conference/journal
```cypher
MATCH (cj:Conference|Journal)<-[:From_c|From_j]-(p:Paper)
RETURN cj.name, COUNT(p)
```
Final query that creates a relationship *Related_to* between conferences/journals and the database community
```cypher
// Match conferences/journals with papers
MATCH (cj:Conference|Journal)<-[]-(:Edition|Volume)<-[]-(p:Paper)
WITH cj, COUNT(p) AS n_papers
// Match conferences/journals with database papers
MATCH (cj)<-[]-(:Edition|Volume)<-[]-(db_p:Paper)<-[]-(:Keyword)-[]->(r:Research_community {name: 'database'})
WITH r, cj, n_papers, COUNT(db_p) AS n_db_papers
WITH r, cj, 1.0 * n_db_papers/ n_papers AS percentage_db_papers
WHERE percentage_db_papers >= 0.9
MERGE (cj)-[:Related_to]->(r)
```
## 3. Top papers of each conference/journal
```cypher

```