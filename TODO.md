### TODO 0.1

- [ ] Filtering
- [ ] Facets
- [ ] Aggregation
- [ ] Advanced query syntax
- [ ] Handle number arrays during indexing, only 1st number is indexed
- [ ] Facet boost
- [ ] Stress Test Framework

### TODO 1.0

- [ ] WAL
- [ ] Simple SQL queries

### TODO 2.0

- [ ] Clustering
- [ ] dtrie rewrite

### Refactor

- [ ] dtrie - msync

### Bugs

- [ ] Two terms matching same word should discard the resulting document, if no other matching word is found
     Eg., "str" : "aaaaa this is nice" with query "aaaaa baaaa" should not match


