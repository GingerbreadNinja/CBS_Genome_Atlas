
JOB = single_genome

# TODO get dir locations from config file (maybe with configure?)

# location that the genomes and outputfile are stored in
GENOME_DIR = /home/panfs/cbs/projects/cge/data/public/genome_sync/accessions/
OUTPUT_DIR = /home/panfs/cbs/projects/cge/data/public/genome_sync/analysis/

# location that executables are stored in
BIN_DIR = /home/people/helen/CBS_Genome_Atlas/analysis/

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

ifndef VERSION
$(error VERSION is not set)
endif

ifndef JOB_UUID
$(error JOB_UUID is not set)
endif

#TODO uuid must be passed through to db_log and stored in jobstep_log table in job_id table

convert_to_fasta: $(ACCESSION)_$(VERSION).fna

$(ACCESSION)_$(VERSION).fna:
	@echo "running convert_to_fasta"
	$(eval LOGID=$(shell python db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=convert_to_fasta --accession=$(ACCESSION) --version=$(VERSION)))
	python convert_gbk_fasta.py -i $(GENOME_DIR)$(ACCESSION)_$(VERSION).gbk -o $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna
	python db_log.py --finish --logid=$(LOGID)

rnammer: $(ACCESSION)_$(VERSION).rrna

$(ACCESSION)_$(VERSION).rrna: $(ACCESSION)_$(VERSION).fna
	@echo "running rnammer"
	$(eval LOGID=$(shell python db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=rnammer --accession=$(ACCESSION) --version=$(VERSION)))
	$(BIN_DIR)rnammer -S bac -m ssu -f $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).rrna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna
	python db_log.py --finish --logid=$(LOGID)

trnascan: $(ACCESSION)_$(VERSION).trna

$(ACCESSION)_$(VERSION).trna: $(ACCESSION)_$(VERSION).fna
	@echo "running trnascan"
	$(eval LOGID=$(shell python db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=trnascan --accession=$(ACCESSION) --version=$(VERSION)))
	/home/people/helen/bin/trnascan-1.4 -o $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).trna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna
	python db_log.py --finish --logid=$(LOGID)

trnascan-stats: $(ACCESSION)_$(VERSION).trna
	@echo "running trnascan-stats"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=trnascan-stats --accession=$(ACCESSION) --version=$(VERSION)))
	python $(BIN_DIR)trnascan-stats.py $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).trna
	python $(BIN_DIR)db_log.py --finish --logid=$(LOGID)

remove-job:
ifndef $(SUCCESS)
	@echo "Done.  Removing job from db"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=remove-job --accession=$(ACCESSION) --version=$(VERSION)))
	python $(BIN_DIR)db_log.py --job_uuid=$(JOB_UUID)
	python $(BIN_DIR)db_log.py --finish --logid=$(LOGID)
endif

clean:
	rm $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).rrna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).trna

