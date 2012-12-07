
-- a job consists of multiple jobsteps
-- eg within the job to process one genome, the jobsteps include converting gbk -> fasta, running rnammer, etc


drop table cron_log;
drop table jobstep_log;
drop table active_job;
drop table job;
drop table jobstep;
drop table job_jobsteps;


-- logs every jobstep (=sub process) that has been run
create table jobstep_log
(
    log_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    start_time DATETIME NOT NULL,
    status enum('Success', 'Failure', 'In Progress', 'Aborted'),
    job_id INTEGER NOT NULL,
    jobstep_id INTEGER NOT NULL,
    job_uuid TEXT NOT NULL,
    accession VARCHAR(255) NOT NULL,
    version INT(11) NOT NULL,
    FOREIGN KEY (job_id) REFERENCES job (job_id), -- (which make file)
    FOREIGN KEY (jobstep_id) REFERENCES jobstep (jobstep_id), -- (which jobstep/command in make file)
    FOREIGN KEY (accession) REFERENCES replicon (accession)
);


-- logs every job that has been submitted to the job queue
-- jobs should be removed from this table when we have their output
create table active_job
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    job_id INTEGER NOT NULL, -- (which make script)
    submission_time DATETIME NOT NULL,
    job_uuid TEXT NOT NULL,
    accession VARCHAR(255) NOT NULL,
    version INT(11) NOT NULL,
    FOREIGN KEY (accession) REFERENCES replicon (accession), -- (null implies all)
    status enum ('Failure', 'In Progress', 'Success')
);

-- logs every time the hourly cron script to enqueue jobs has been run
create table cron_log
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    start_time DATETIME NOT NULL,
    responsible enum ('cron', 'oneoff', 'backfill')
);

create table job
(
    job_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    job_name TEXT,
    description TEXT
);

create table jobstep
(
    jobstep_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    job_name TEXT,
    description TEXT
);

-- a job has many jobsteps
create table job_jobsteps
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    job_id INTEGER,
    jobstep_id INTEGER,
    FOREIGN KEY (job_id) REFERENCES job (job_id),
    FOREIGN KEY (jobstep_id) REFERENCES jobstep (jobstep_id)
);


insert into job (job_name, description) values ('single_genome', 'All the steps to process one single genome for display.');
insert into jobstep (jobstep_name, description) values ('convert_to_fasta', 'Converts the downloaded genebank format to FASTA format for further processing.');
insert into jobstep (jobstep_name, description) values ('rnammer', 'Runs RNAmmer to predict rRNA structures.  Relies on having the data in FASTA format.');
insert into jobstep (jobstep_name, description) values ('trnascan', 'Runs trnascan-1.4 to predict tRNA structures.  Relies on having the data in FASTA format.');
insert into jobstep (jobstep_name, description) values ('trnascan-stats', 'Takes files generated by trnascan-1.4 and writes the data into the database.');
insert into jobstep (jobstep_name, description) values ('remove-job', 'Removes the job entry from the active_job table.');
insert into jobstep (jobstep_name, description) values ('rnammer-stats', 'Takes file generated by rnammer 1.2 and writes them to the database.');
insert into job_jobsteps( job_id, jobstep_id) values (1, 1);
insert into job_jobsteps( job_id, jobstep_id) values (1, 2);
insert into job_jobsteps( job_id, jobstep_id) values (1, 3);
insert into job_jobsteps( job_id, jobstep_id) values (1, 4);
insert into job_jobsteps( job_id, jobstep_id) values (1, 5);
insert into job_jobsteps( job_id, jobstep_id) values (1, 6);

create or replace view accession_version_by_name as select bioproject.bioproject_id, modify_date, concat(accession, "_", version) as av, accession, version, replicon_type, genome_name from genome, replicon, bioproject where genome.bioproject_id = bioproject.bioproject_id and genome.genome_id=replicon.genome_id;

create or replace view log as select jobstep_log.log_id, jobstep_log.accession, jobstep_log.version, jobstep_name as step_name, jobstep_log.status as step_status, jobstep_log.start_time as step_start_time, job_name, jobstep_log.job_uuid, active_job.status as job_status, active_job.submission_time as job_start_time from active_job, jobstep_log, job, jobstep where active_job.job_uuid = jobstep_log.job_uuid and job.job_id= jobstep_log.job_id and jobstep.jobstep_id=jobstep_log.jobstep_id;

create or replace view displaygenome_jobs as select * from log;

-- this doesn't do the right thing -- status is not correct
select accession, version, status, submission_time from active_job where (accession, submission_time) in (select accession, max(submission_time) from active_job group by accession) order by accession

create or replace view plasmid as select genome_id, coalesce(count(replicon.accession), 0) as plasmid_count from replicon where replicon_type = "PLASMID" group by genome_id;
create or replace view chromosome as select genome_id, coalesce(count(replicon.accession), 0) as chromosome_count from replicon where replicon_type = "CHROMOSOME" group by genome_id;
create or replace view contig as select genome_id, coalesce(sum(stat_number_of_contigs), 0) as contig_count from replicon group by genome_id;
create or replace view nonstd_bases as select genome_id, coalesce(sum(stat_number_nonstd_bases), 0) as nonstd_count from replicon group by genome_id;

create or replace view trna_count as select genome_id, count(*) as trna_count from trna, replicon where trna.accession = replicon.accession and trna.version = replicon.version group by genome_id;

--create or replace view rrna_count as select genome_id, count(*) as rrna_count from rrna, replicon where rrna.accession = replicon.accession and rrna.version = replicon.version group by genome_id;

create or replace view rrna_count as select genome_id, count(*) as rrna_count, 100*avg((2*length(sequence) - length(replace(sequence, 'g', '')) - length(replace(sequence, 'c', ''))) / length(sequence)) AS rrna_percent_at from rrna, replicon where rrna.accession = replicon.accession and rrna.version = replicon.version group by genome_id;

create or replace view rrna_aggregate as select accession, avg((2*length(sequence) - length(replace(sequence, 'g', '')) - length(replace(sequence, 'c', ''))) / length(sequence)) AS avg_percentat from rrna group by accession;


create table genome_external_data (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    genome_id INTEGER,
    doubling_time_minutes INTEGER,
    optimal_growth_temp INTEGER,
    FOREIGN KEY (genome_id) REFERENCES genome (genome_id)
);

create or replace view genome_stats as 
select 
bioproject.modify_date,
replicon.genome_id, 
genome.tax_id, 
genome.bioproject_id, 
genome_name, 
greatest (0.1, 1+.1*(0-ceiling(100*sum(stat_number_nonstd_bases)/sum(stat_size_bp)))) as score_nonstd,
coalesce(chromosome.chromosome_count, 0) as chromosome_count, 
coalesce(plasmid.plasmid_count, 0) as plasmid_count,
count(replicon.accession) as replicon_count,
coalesce(contig.contig_count, 0) as contig_count,
greatest(0.1, 1-0.2*ceiling((1000000*greatest(0, (coalesce(contig.contig_count, 0) - count(replicon.accession)))/sum(stat_size_bp))/25)) as score_contig,
greatest (0.1, 1 - (1- greatest (0.1, 1+.1*(0-ceiling(100*sum(stat_number_nonstd_bases)/sum(stat_size_bp))))
) - (1 - greatest(0.1, 1-0.2*ceiling((1000000*greatest(0, (coalesce(contig.contig_count, 0) - count(replicon.accession)))/sum(stat_size_bp))/25)) 
)) as score,
sum(stat_size_bp) as total_bp, 
stat_number_nonstd_bases as nonstd_bp,
stat_number_nonstd_bases/sum(stat_size_bp)*100 as percent_nonstd_bp,
sum(stat_number_of_genes) as gene_count,
format(1000*sum(stat_number_of_genes) / sum(stat_size_bp),3) as gene_density,
format(sum(stat_perc_at * stat_size_bp)/sum(stat_size_bp),1) as percent_at, 
group_concat(concat(replicon.accession, "_", replicon.version) SEPARATOR ' ') as accessions,
rrna_count,
trna_count
from replicon 
left outer join plasmid on replicon.genome_id = plasmid.genome_id 
left outer join chromosome on replicon.genome_id = chromosome.genome_id 
left outer join contig on replicon.genome_id = contig.genome_id
left outer join genome on genome.genome_id = replicon.genome_id 
left outer join trna_count on genome.genome_id = trna_count.genome_id
left outer join rrna_count on genome.genome_id = rrna_count.genome_id
left outer join bioproject on genome.bioproject_id = bioproject.bioproject_id
group by replicon.genome_id;

create or replace view displaygenome_genome_stats as select * from genome_stats;




create table tax_path (
    path_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    tax_id INTEGER NOT NULL,
    accession VARCHAR(20) NOT NULL,
    version INTEGER NOT NULL
);

create table tax_stats (
    row_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    tax_id INTEGER,
    modify_date DATE,
    genome_id INTEGER,
    accession VARCHAR(20),
    version INTEGER,
    bioproject_id INTEGER,
    genome_name VARCHAR(255),
    score_nonstd_sum INTEGER,
    chromosome_count INTEGER,
    plasmid_count INTEGER, 
    replicon_count INTEGER,
    contig_count INTEGER,
    score_contig_sum INTEGER,
    score_sum INTEGER,
    total_bp BIGINT,
    nonstd_bp BIGINT,
    gene_count INTEGER,
    at_bp BIGINT,
    rrna_count BIGINT,
    trna_count BIGINT,
    genome_count INTEGER,
    tax_name VARCHAR(255)
);

create or replace view displaygenome_tax_stats as select * from tax_stats;


create or replace view replicon_stats as
select
replicon.genome_id, replicon.accession, replicon.version, stat_size_bp, stat_number_nonstd_bases, stat_perc_at, stat_number_of_genes, stat_number_of_contigs, replicon_type, replicon_id, trna_count_accession, rrna_count_accession,
format(1000*stat_number_of_genes/stat_size_bp,3) as gene_density
from replicon
left outer join trna_count_accession on trna_count_accession.accession = replicon.accession
left outer join rrna_count_accession on rrna_count_accession.accession = replicon.accession
group by replicon.accession order by replicon.stat_size_bp desc;

create or replace view displaygenome_replicon_stats as select * from replicon_stats;



create table phyla 
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    phyla_name VARCHAR(255) NOT NULL,
    tax_id INTEGER NOT NULL,
    colour VARCHAR(255)
);

/* perhaps faster, but wrong version
create or replace view genome_stats as 
select 
genome.genome_id, 
genome.tax_id, 
bioproject_id, 
genome_name, 
chromosome_count, 
plasmid_count, 
count(replicon.accession) as replicon_count, 
contig_count,
sum(stat_size_bp) as total_length, 
sum(stat_perc_at * stat_size_bp)/sum(stat_size_bp) as percent_at, 
nonstd_count/total_length as perc_nonstd_bases,
group_concat(concat(replicon.accession, "_", replicon.version)) as accessions 
from genome, replicon, plasmid, chromosome, contig where genome.genome_id = replicon.genome_id and genome.genome_id = plasmid.genome_id and genome.genome_id = chromosome.genome_id and genome.genome_id = contig.genome_id and stat_size_bp is not null group by genome.genome_id;
*/


-- does not work as expected: pulling in trnas duplicates rows, and distinct isn't enough to get rid of them in the sums.
-- will need to make other temp tables to separate chromosome and plasmid counts as well
-- create or replace view genome_stats as select genome.genome_id, genome_name, count(replicon.accession) as replicon_count, group_concat(distinct(concat(replicon.accession, "_", replicon.version))) as accessions, sum(stat_size_bp) as total_length, sum(stat_perc_at * stat_size_bp)/sum(stat_size_bp) as percent_at, count(trna.id) as trna_count from genome, replicon, trna where genome.genome_id = replicon.genome_id and trna.accession = replicon.accession and stat_size_bp is not null group by genome.genome_id;

create table trna
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    accession varchar(255) NOT NULL,
    FOREIGN KEY (accession) REFERENCES replicon (accession),
    version INT(11) NOT NULL,
    start_location INTEGER NOT NULL,
    end_location INTEGER NOT NULL,
    complementary_strand enum ('yes', 'no'),
    amino_acid enum ('Ala', 'Arg', 'Asn', 'Asp', 'Cys', 'Gln', 'Glu', 'Gly', 'His', 'Ile', 'Leu', 'Lys', 'Met', 'Phe', 'Pro', 'Ser', 'Thr', 'Trp', 'Tyr', 'Val', 'Sup'),
    anti_codon enum ('aaa', 'aac', 'aag', 'aat', 'aca', 'acc', 'acg', 'act', 'aga', 'agc', 'agg', 'agt', 'ata', 'atc', 'atg', 'att', 'caa', 'cac', 'cag', 'cat', 'cca', 'ccc', 'ccg', 'cct', 'cga', 'cgc', 'cgg', 'cgt', 'cta', 'ctc', 'ctg', 'ctt', 'gaa', 'gac', 'gag', 'gat', 'gca', 'gcc', 'gcg', 'gct', 'gga', 'ggc', 'ggg', 'ggt', 'gta', 'gtc', 'gtg', 'gtt', 'taa', 'tac', 'tag', 'tat', 'tca', 'tcc', 'tcg', 'tct', 'tga', 'tgc', 'tgg', 'tgt', 'tta', 'ttc', 'ttg', 'ttt'),
    sequence TEXT
);

create table rrna
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    accession varchar(255) NOT NULL,
    FOREIGN KEY (accession) REFERENCES replicon (accession),
    version INT(11) NOT NULL,
    start_location INTEGER NOT NULL,
    end_location INTEGER NOT NULL,
    complementary_strand enum ('yes', 'no'),
    molecule enum('5s', '23s', '16s'),
    score FLOAT,
    sequence TEXT
)

create or replace view rrna_count_accession as select accession, count(*) as rrna_count_accession from rrna group by accession;
create or replace view trna_count_accession as select accession, count(*) as trna_count_accession from trna group by accession;



create table rrna_alignment
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    rrna1_id INTEGER,
    rrna2_id INTEGER,
    score FLOAT
)

-- get the number of each nucleotide in the rrna sequence
select accession, length(sequence), length(replace(sequence, 'g', '')) as no_g, length(replace(sequence, 'c', '')) as no_c, length(replace(sequence, 't', '')) as no_t, length(replace(sequence, 'a', '')) as no_a, (2*length(sequence) - length(replace(sequence, 'g', '')) - length(replace(sequence, 'c', ''))) / length(sequence) AS percentat from rrna where accession='FQ312030';


-- django likes to give tables names prefixed by the app name, so make it happy with these:
create or replace view displaygenome_rrna as select * from rrna;
create or replace view displaygenome_trna as select * from trna;

