DROP DATABASE IF EXISTS cbs_genome_atlas_dev;

CREATE DATABASE cbs_genome_atlas_dev;

USE cbs_genome_atlas_dev;

CREATE TABLE taxonomy
(
    tax_id INTEGER PRIMARY KEY,
    taxon_name VARCHAR(255) NOT NULL,
    left_id INTEGER,
    depth INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (left_id) REFERENCES taxonomy (tax_id)    
);

CREATE TABLE genome
(
    genome_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    tax_id INTEGER REFERENCES taxonomy (tax_id), -- If it's null that means we need to look it up!
    genome_name VARCHAR(255) NOT NULL,
    genome_validity ENUM('ONLINE', 'VALID','WARNINGS','INVALID', 'RECONCILED') -- Added reconciled so that we don't continuously flag a project and mark it as warnings/errors for now
);

CREATE TABLE bioproject
(
    genome_id INTEGER references genome (genome_id), -- Left this here (one join to get to project table joined with bioproject rather than two...). Willing to change to separate table if desired
    bioproject_id INTEGER PRIMARY KEY,
    bioproject_status ENUM('COMPLETE','SCAFFOLDS_CONTIGS','NO_DATA','SRA_TRACES'),
    release_date DATE,
    modify_date DATE
);

/* REPLICON
 * I have left all the statistics in a per-replicon fashion. This way, if one replicon is updated,
 * we must only re-run rnammer (or whatever the statistic is) on the individual gbk file and not
 * the entire genome
 */
CREATE TABLE replicon
(
    accession VARCHAR(255) PRIMARY KEY, -- Left version field separate
    genome_id INTEGER NOT NULL REFERENCES genome (genome_id), -- I don't think nulls should ever happen here..?
    version INTEGER NOT NULL,
    stat_5s_count INTEGER, -- NULL allowed so we can check what needs to be run
    stat_16s_count INTEGER, -- NULL allowed so we can check what needs to be run
    stat_23s_count INTEGER, -- NULL allowed so we can check what needs to be run
    stat_size_bp INTEGER, -- NULL allowed so we can check what needs to be run
    stat_perc_at DECIMAL, -- NULL allowed so we can check what needs to be run
    stat_number_of_genes INTEGER, -- NULL allowed so we can check what needs to be run
    replicon_type ENUM('CHROMOSOME','PLASMID') NOT NULL
);

CREATE VIEW project_statistics AS
(
    SELECT
        genome_id,
        tax_id,
        genome_name,
        genome_validity,
        SUM(stat_5s_count) as stat_5s_count_total,
        SUM(stat_16s_count) as stat_16s_count_total,
        SUM(stat_23s_count) as stat_23s_count_total,
        SUM(stat_size_bp) as stat_size_bp_total,
        SUM(stat_perc_at * CAST(stat_size_bp as DECIMAL))/cast(SUM(stat_size_bp) as DECIMAL) as stat_perc_at_total
    FROM
        replicon r
    JOIN
        genome g
    USING (genome_id)
    GROUP BY r.genome_id
);
