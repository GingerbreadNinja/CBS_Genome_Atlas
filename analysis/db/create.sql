
-- a job consists of multiple jobsteps
-- eg within the job to process one genome, the jobsteps include converting gbk -> fasta, running rnammer, etc


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
    status enum('Successful', 'Error', 'In Progress', 'Aborted'),
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
    status enum ('Permanent Failure', 'In Progress', 'Success')
);

-- logs every time the script to enqueue jobs has been run
create table cron_log
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    start_time DATETIME NOT NULL,
    responsible enum ('cron', 'oneoff')
);

create table job
(
    job_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name TEXT,
    description TEXT
);

create table jobstep
(
    jobstep_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name TEXT,
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


insert into job (name, description) values ('single_genome', 'All the steps to process one single genome for display.');
insert into jobstep (name, description) values ('convert_to_fasta', 'Converts the downloaded genebank format to FASTA format for further processing.');
insert into jobstep (name, description) values ('rnammer', 'Runs RNAmmer to predict rRNA structures.  Relies on having the data in FASTA format.');
insert into jobstep (name, description) values ('trnascan', 'Runs trnascan-1.4 to predict tRNA structures.  Relies on having the data in FASTA format.');
insert into jobstep (name, description) values ('trnascan-stats', 'Takes files generated by trnascan-1.4 and writes the data into the database.');
insert into jobstep (name, description) values ('remove-job', 'Removes the job entry from the active_job table.');
insert into job_jobsteps( job_id, jobstep_id) values (1, 1);
insert into job_jobsteps( job_id, jobstep_id) values (1, 2);
insert into job_jobsteps( job_id, jobstep_id) values (1, 3);
insert into job_jobsteps( job_id, jobstep_id) values (1, 4);
insert into job_jobsteps( job_id, jobstep_id) values (1, 5);


create table trna
(
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    accession varchar(255) NOT NULL,
    FOREIGN KEY (accession) REFERENCES replicon (accession),
    version INT(11) NOT NULL,
    start_location INTEGER NOT NULL,
    end_location INTEGER NOT NULL,
    amino_acid enum ('Ala', 'Arg', 'Asn', 'Asp', 'Cys', 'Gln', 'Glu', 'Gly', 'His', 'Ile', 'Leu', 'Lys', 'Met', 'Phe', 'Pro', 'Ser', 'Thr', 'Trp', 'Tyr', 'Val'),
    anti_codon enum ('aaa', 'aac', 'aag', 'aat', 'aca', 'acc', 'acg', 'act', 'aga', 'agc', 'agg', 'agt', 'ata', 'atc', 'atg', 'att', 'caa', 'cac', 'cag', 'cat', 'cca', 'ccc', 'ccg', 'cct', 'cga', 'cgc', 'cgg', 'cgt', 'cta', 'ctc', 'ctg', 'ctt', 'gaa', 'gac', 'gag', 'gat', 'gca', 'gcc', 'gcg', 'gct', 'gga', 'ggc', 'ggg', 'ggt', 'gta', 'gtc', 'gtg', 'gtt', 'taa', 'tac', 'tag', 'tat', 'tca', 'tcc', 'tcg', 'tct', 'tga', 'tgc', 'tgg', 'tgt', 'tta', 'ttc', 'ttg', 'ttt'),
    sequence TEXT
);

/*
create table rRNA
(
    FOREIGN KEY (accession) REFERENCES replicon (accession),
    start_location INTEGER NOT NULL,
    end_location INTEGER NOT NULL,
    type enum('5S', '23S', '16S'),
    score FLOAT,
    sequence TEXT
)

*/

