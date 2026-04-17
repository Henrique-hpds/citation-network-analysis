statements = [

# CONSTRAINTS DE UNICIDADE

# Artigos
"CREATE CONSTRAINT article_openalex_id_unique IF NOT EXISTS "
"FOR (a:Article) "
"REQUIRE a.openalex_id IS UNIQUE",

# Autores
"CREATE CONSTRAINT author_openalex_id_unique IF NOT EXISTS "
"FOR (a:Author) "
"REQUIRE a.openalex_id IS UNIQUE",

# Instituições
"CREATE CONSTRAINT institution_openalex_id_unique IF NOT EXISTS "
"FOR (i:Institution) "
"REQUIRE i.openalex_id IS UNIQUE",

# Venues (journal, conference, source)
"CREATE CONSTRAINT venue_openalex_id_unique IF NOT EXISTS "
"FOR (v:Venue) "
"REQUIRE v.openalex_id IS UNIQUE",

# Conceitos / áreas
"CREATE CONSTRAINT concept_openalex_id_unique IF NOT EXISTS "
"FOR (c:Concept) "
"REQUIRE c.openalex_id IS UNIQUE",

# ÍNDICES PARA CONSULTA

#  Artigos
"CREATE INDEX article_title_index IF NOT EXISTS "
"FOR (a:Article) "
"ON (a.title)",

"CREATE INDEX article_year_index IF NOT EXISTS "
"FOR (a:Article) "
"ON (a.publication_year)",

"CREATE INDEX article_doi_index IF NOT EXISTS "
"FOR (a:Article) "
"ON (a.doi)",

"CREATE INDEX article_is_ic_index IF NOT EXISTS "
"FOR (a:Article) "
"ON (a.is_from_ic)",

"CREATE INDEX article_high_impact_index IF NOT EXISTS "
"FOR (a:Article) "
"ON (a.is_high_impact)",

# Autores
"CREATE INDEX author_name_index IF NOT EXISTS "
"FOR (a:Author) "
"ON (a.display_name)",

"CREATE INDEX author_is_ic_index IF NOT EXISTS "
"FOR (a:Author) "
"ON (a.is_ic_researcher)",

# Instituições
"CREATE INDEX institution_name_index IF NOT EXISTS "
"FOR (i:Institution) "
"ON (i.display_name)",

# Venues
"CREATE INDEX venue_name_index IF NOT EXISTS "
"FOR (v:Venue) "
"ON (v.display_name)",

# Conceitos
"CREATE INDEX concept_name_index IF NOT EXISTS "
"FOR (c:Concept) "
"ON (c.display_name)",

]


enterprise_only_statements = [

# CONSTRAINTS DE EXISTÊNCIA

# Recomendado exigir IDs principais
"CREATE CONSTRAINT article_openalex_id_exists IF NOT EXISTS "
"FOR (a:Article) "
"REQUIRE a.openalex_id IS NOT NULL",

"CREATE CONSTRAINT author_openalex_id_exists IF NOT EXISTS "
"FOR (a:Author) "
"REQUIRE a.openalex_id IS NOT NULL",

"CREATE CONSTRAINT institution_openalex_id_exists IF NOT EXISTS "
"FOR (i:Institution) "
"REQUIRE i.openalex_id IS NOT NULL",

"CREATE CONSTRAINT venue_openalex_id_exists IF NOT EXISTS "
"FOR (v:Venue) "
"REQUIRE v.openalex_id IS NOT NULL",

"CREATE CONSTRAINT concept_openalex_id_exists IF NOT EXISTS "
"FOR (c:Concept) "
"REQUIRE c.openalex_id IS NOT NULL",

]
