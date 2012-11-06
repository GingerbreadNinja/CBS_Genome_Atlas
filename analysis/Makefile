
JOB = single_genome

# location that the genomes and outputfile are stored in
GENOME_DIR = ../../genomes/

# location that executables are stored in
BIN_DIR = ./

all: single_genome

single_genome: convert_to_fasta trnascan trnascan-stats remove-job
#single_genome: convert_to_fasta rnammer rnastats rnaselfalignment rnacrossalignment trnascan

# variables ACCESSION and JOB_UUID must be overridden on the make command line for each accession number, for example:
# make ACCESSION=CP0000202 JOB_UUID=uuid all
# this assumes the existence of example.gbk as a genebank formatted file, and will create additional files as example.*

# remove-job must be called if all steps were successful, and must be the last recipe processed.  it deregisters this job from the database once everything is done.

ifndef ACCESSION
$(error ACCESSION is not set)
endif

ifndef JOB_UUID
$(error JOB_UUID is not set)
endif

#TODO uuid must be passed through to db_log and stored in jobstep_log table in job_id table

convert_to_fasta: $(ACCESSION).fna

$(ACCESSION).fna:
	@echo "running convert_to_fasta"
	$(eval LOGID=$(shell python db_log.py --start --job=$(JOB) --jobstep=convert_to_fasta --accession=$(ACCESSION)))
	python convert_gbk_fasta.py -i $(GENOME_DIR)$(ACCESSION).gbk -o $(GENOME_DIR)$(ACCESSION).fna
	python db_log.py --finish --logid=$(LOGID)

rnammer: $(ACCESSION).rrna

$(ACCESSION).rrna: $(ACCESSION).fna
	@echo "running rnammer"
	$(eval LOGID=$(shell python db_log.py --start --job=$(JOB) --jobstep=rnammer --accession=$(ACCESSION)))
	$(BIN_DIR)rnammer -S bac -m ssu -f $(GENOME_DIR)$(ACCESSION).rrna $(GENOME_DIR)$(ACCESSION).fna
	python db_log.py --finish --logid=$(LOGID)

trnascan: $(ACCESSION).trna

$(ACCESSION).trna: $(ACCESSION).fna
	@echo "running trnascan"
	$(eval LOGID=$(shell python db_log.py --start --job=$(JOB) --jobstep=trnascan --accession=$(ACCESSION)))
	/home/people/helen/bin/trnascan-1.4 -o $(GENOME_DIR)$(ACCESSION).trna $(GENOME_DIR)$(ACCESSION).fna
	python db_log.py --finish --logid=$(LOGID)

trnascan-stats: $(ACCESSION).trna
	@echo "running trnascan-stats"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --jobstep=trnascan-stats --accession=$(ACCESSION)))
	python $(BIN_DIR)trnascan-stats.py $(GENOME_DIR)$(ACCESSION).trna
	python $(BIN_DIR)db_log.py --finish --logid=$(LOGID)

remove-job:
ifndef $(SUCCESS)
	@echo "Done.  Removing job from db"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --jobstep=remove-job --accession=$(ACCESSION)))
	python $(BIN_DIR)remove_job.py --job=$(JOB) --accession=$(ACCESSION)
	python $(BIN_DIR)db_log.py --finish --logid=$(LOGID)
endif

#TODO this can never be called by make clean because of check on ACCESSION above
clean:
	rm $(GENOME_DIR)*.fna $(GENOME_DIR)*.rrna

