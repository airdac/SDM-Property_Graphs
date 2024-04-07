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
MERGE (:Research_community {name: 'database'})
```
```cypher
MATCH (k:Keyword)
WHERE k.tag IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
MATCH (r:Research_community)
MERGE (k)-[:From_r]->(r)
```