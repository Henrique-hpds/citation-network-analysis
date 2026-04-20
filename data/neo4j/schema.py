statements = [
    # -------------------------------------------------------------------------
    # CONSTRAINTS DE UNICIDADE
    # -------------------------------------------------------------------------

    # Articles
    "CREATE CONSTRAINT article_openalex_id_unique IF NOT EXISTS "
    "FOR (a:Article) "
    "REQUIRE a.openalex_id IS UNIQUE",

    # Authors
    "CREATE CONSTRAINT author_openalex_id_unique IF NOT EXISTS "
    "FOR (a:Author) "
    "REQUIRE a.openalex_id IS UNIQUE",

    # Institutions
    "CREATE CONSTRAINT institution_openalex_id_unique IF NOT EXISTS "
    "FOR (i:Institution) "
    "REQUIRE i.openalex_id IS UNIQUE",

    # Venues
    "CREATE CONSTRAINT venue_openalex_id_unique IF NOT EXISTS "
    "FOR (v:Venue) "
    "REQUIRE v.openalex_id IS UNIQUE",

    # Topics
    "CREATE CONSTRAINT topic_openalex_id_unique IF NOT EXISTS "
    "FOR (t:Topic) "
    "REQUIRE t.openalex_id IS UNIQUE",

    # Funders
    "CREATE CONSTRAINT funder_openalex_id_unique IF NOT EXISTS "
    "FOR (f:Funder) "
    "REQUIRE f.openalex_id IS UNIQUE",

    # -------------------------------------------------------------------------
    # ÍNDICES DE PROPRIEDADE — nós
    # -------------------------------------------------------------------------

    # Article
    "CREATE INDEX article_doi IF NOT EXISTS "
    "FOR (a:Article) ON (a.doi)",

    "CREATE INDEX article_publication_year IF NOT EXISTS "
    "FOR (a:Article) ON (a.publication_year)",

    "CREATE INDEX article_cited_by_count IF NOT EXISTS "
    "FOR (a:Article) ON (a.cited_by_count)",

    "CREATE INDEX article_is_retracted IF NOT EXISTS "
    "FOR (a:Article) ON (a.is_retracted)",

    # Author
    "CREATE INDEX author_orcid IF NOT EXISTS "
    "FOR (a:Author) ON (a.orcid)",

    "CREATE INDEX author_display_name IF NOT EXISTS "
    "FOR (a:Author) ON (a.display_name)",

    # Institution
    "CREATE INDEX institution_ror IF NOT EXISTS "
    "FOR (i:Institution) ON (i.ror)",

    "CREATE INDEX institution_country_code IF NOT EXISTS "
    "FOR (i:Institution) ON (i.country_code)",

    # Venue
    "CREATE INDEX venue_issn_l IF NOT EXISTS "
    "FOR (v:Venue) ON (v.issn_l)",

    # Topic
    "CREATE INDEX topic_field_id IF NOT EXISTS "
    "FOR (t:Topic) ON (t.field_id)",

    "CREATE INDEX topic_domain_id IF NOT EXISTS "
    "FOR (t:Topic) ON (t.domain_id)",

    # -------------------------------------------------------------------------
    # ÍNDICES DE PROPRIEDADE — relacionamentos
    # -------------------------------------------------------------------------

    # AUTHORED_BY: author_position is frequently filtered (e.g. first authors only)
    "CREATE INDEX authored_by_position IF NOT EXISTS "
    "FOR ()-[r:AUTHORED_BY]-() ON (r.author_position)",

    # AUTHORED_BY: quickly find corresponding authors
    "CREATE INDEX authored_by_is_corresponding IF NOT EXISTS "
    "FOR ()-[r:AUTHORED_BY]-() ON (r.is_corresponding)",

    # HAS_TOPIC: distinguish primary from secondary topics
    "CREATE INDEX has_topic_is_primary IF NOT EXISTS "
    "FOR ()-[r:HAS_TOPIC]-() ON (r.is_primary)",
]