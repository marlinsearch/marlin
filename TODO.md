### TODO 0.1

- [x] Filtering
- [ ] Facets
- [ ] Restrict facets
- [ ] Get specific facets, instead of everything
- [ ] Boolean Facets
- [ ] Parial Scanning during queries
- [ ] Mapping updates on reconfiguration
- [ ] Sorting / Ranking by numeric value
- [ ] CRUD
- [ ] Python tests
- [ ] Stress Test Framework

### TODO 0.2

- [ ] Aggregation
- [ ] Facet boost
- [ ] Handle number arrays during indexing, only 1st number is indexed

### TODO 1.0

- [ ] WAL
- [ ] Advanced query syntax
- [ ] Simple SQL queries

### TODO 2.0

- [ ] Clustering
- [ ] dtrie rewrite

### Refactor

- [ ] dtrie - msync

### Bugs

- [ ] Two terms matching same word should discard the resulting document, if no other matching word is found
     Eg., "str" : "aaaaa this is nice" with query "aaaaa baaaa" should not match
- [ ] Segfault when an instance of marlin is already running on configured port
