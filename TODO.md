### TODO 0.1

- [x] Filtering
- [x] Facets
- [x] Sorting / Ranking by numeric value
- [x] CRUD
- [x] Basic Stress Test Framework
- [ ] Keys
- [ ] Partial Scanning during queries
- [ ] Python tests
- [ ] Mapping updates on reconfiguration
- [ ] Reindexing
- [ ] Documentation
- [ ] Website

### TODO 0.2

- [ ] Bulk updates
- [ ] Delete by query
- [ ] Restrict facets
- [ ] Restrict fields to return
- [ ] Restrict fields to search
- [ ] Get specific facets, instead of everything
- [ ] Boolean Facets
- [ ] Management web ui
- [ ] Aggregation
- [ ] Facet boost
- [ ] Handle numeric arrays during indexing, only 1st number is indexed
- [ ] Better Stress Test Framework

### TODO 1.0

- [ ] WAL
- [ ] Advanced query syntax
- [ ] Simple SQL queries

### TODO 2.0

- [ ] Clustering
- [ ] dtrie rewrite

### Refactor

- [ ] dtrie - msync
- [ ] http_error / api_bad_request use just one

### Bugs

- [ ] Two terms matching same word should discard the resulting document, if no other matching word is found
     Eg., "str" : "aaaaa this is nice" with query "aaaaa baaaa" should not match
- [ ] Segfault when an instance of marlin is already running on configured port


### In Progress

Python tests

### Tests to complete

- facet.robot
- sort.robot
