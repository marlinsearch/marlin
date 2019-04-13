### TODO 0.1

- [x] Filtering
- [x] Facets
- [x] Sorting / Ranking by numeric value
- [x] CRUD
- [x] Basic Stress Test Framework
- [x] Keys
- [x] Python tests
- [x] Partial Scanning during queries
- [x] Highlighting
- [x] Snippets
- [x] Restrict fields to return
- [ ] Reindexing
- [ ] Mapping updates on reconfiguration
- [ ] Documentation
- [ ] Website

### TODO 0.2

- [ ] Review & Cleanup
- [ ] Aggregation
- [ ] Bulk updates
- [ ] Better logging
- [ ] Delete by query
- [ ] Restrict facets
- [ ] Restrict fields to search
- [ ] Get specific facets, instead of everything
- [ ] Boolean Facets
- [ ] Management web ui
- [ ] Facet boost
- [ ] Handle numeric arrays during indexing, only 1st number is indexed
- [ ] Better Stress Test Framework

### TODO 0.3
- [ ] Better analyzers, stop words, plurals, wiktionary etc.,
- [ ] Synonyms
- [ ] Group results

### TODO 0.4
- [ ] Split word match 'setup' matches 'set up' 

### TODO 1.0

- [ ] WAL
- [ ] Advanced query syntax
- [ ] Phrase search

### TODO 2.0

- [ ] Clustering
- [ ] dtrie rewrite
- [ ] analyzer rewrite
- [ ] Simple SQL queries

### Refactor

- [ ] http_error / api_bad_request use just one
- [ ] dtrie - msync
- [ ] Analyzers - Rewrite

### Bugs

- [ ] Two terms matching same word should discard the resulting document, if no other matching word is found
     Eg., "str" : "aaaaa this is nice" with query "aaaaa baaaa" should not match


### In Progress

- Documentation
- Website


### Tests to complete

- [ ] facet.robot
- [ ] sort.robot
- [ ] rank_terms.robot
