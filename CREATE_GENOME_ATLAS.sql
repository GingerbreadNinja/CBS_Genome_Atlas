DROP DATABASE IF EXISTS cbs_genome_atlas_test;

CREATE DATABASE cbs_genome_atlas_test;

USE cbs_genome_atlas_test;

CREATE TABLE taxonomy
(
    tax_id INTEGER PRIMARY KEY,
    taxon_name VARCHAR(255) NOT NULL,
    left_id INTEGER,
    depth INTEGER NOT NULL DEFAULT 0    
);

CREATE TABLE project
(
    project_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    tax_id INTEGER REFERENCES taxonomy (tax_id), # If it's null that means we need to look it up!
    project_name VARCHAR(255) NOT NULL,
    project_status ENUM('COMPLETE','SCAFFOLDS_CONTIGS','NO_DATA','SRA_TRACES'), # Not sure if this is quite what we want here. Is this universal?
    project_validity ENUM('VALID','WARNINGS','INVALID', 'RECONCILED') # Added reconciled so that we don't continuously flag a project and mark it as warnings/errors for now
);

CREATE TABLE bioproject
(
    project_id INTEGER references project (project_id), # Left this here (one join to get to project table joined with bioproject rather than two...). Willing to change to separate table if desired
    bioproject_id INTEGER PRIMARY KEY,
    release_date DATE,
    modify_date DATE
);

## REPLICON
# I have left all the statistics in a per-replicon fashion. This way, if one replicon is updated,
# we must only re-run rnammer (or whatever the statistic is) on the individual gbk file and not
# the entire genome

CREATE TABLE replicon
(
    locus VARCHAR(255) PRIMARY KEY, #I'm leaving this as such for now in hopes that we'll get warnings on insert when we have the same accession but a different version number...
    project_id INTEGER NOT NULL REFERENCES project (project_id), #I don't think nulls should ever happen here..?
    version INTEGER NOT NULL,
    stat_5s_count INTEGER, # NULL allowed so we can check what needs to be run
    stat_16s_count INTEGER, # NULL allowed so we can check what needs to be run
    stat_23s_count INTEGER, # NULL allowed so we can check what needs to be run
    stat_size_bp INTEGER, # NULL allowed so we can check what needs to be run
    stat_perc_at DECIMAL, # NULL allowed so we can check what needs to be run
    stat_number_of_genes INTEGER, # NULL allowed so we can check what needs to be run
    replicon_type ENUM('CHROMOSOME','PLASMID') NOT NULL DEFAULT 'PLASMID'
);

CREATE VIEW project_statistics AS
(
    SELECT
        project_id,
        SUM(stat_5s_count) as stat_5s_count_total,
        SUM(stat_16s_count) as stat_16s_count_total,
        SUM(stat_23s_count) as stat_23s_count_total,
        SUM(stat_size_bp) as stat_size_bp_total,
        SUM(stat_perc_at * CAST(stat_size_bp as DECIMAL))/cast(SUM(stat_size_bp) as DECIMAL) as stat_perc_at_total
    FROM
        replicon
    GROUP BY project_id
);