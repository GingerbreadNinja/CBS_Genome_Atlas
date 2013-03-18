
SHELL = /bin/bash

JOB = single_genome

# TODO get dir locations from config file (maybe with configure?)

# location that the genomes and outputfile are stored in
GENOME_DIR = /home/panfs/cbs/projects/cge/data/public/genome_sync/accessions/
OUTPUT_DIR = /home/panfs/cbs/projects/cge/data/public/genome_sync/analysis/

# location that executables are stored in
BIN_DIR = /home/people/helen/CBS_Genome_Atlas/analysis/

# variables ACCESSION and JOB_UUID must be overridden on the make command line for each accession number, for example:
# make -k -i ACCESSION=CP0000202 JOB_UUID=uuid all
# this assumes the existence of example.gbk as a genebank formatted file, and will create additional files as example.*

# remove-job must be called if all steps were successful, and must be the last recipe processed.  it deregisters this job from the database once everything is done.

# you should run this as make -k -i
# -k -> continue through as many recipes as possible
# -i -> ignore errors, they will be handled in each recipe

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


all: single_genome

#single_genome: convert_to_fasta rnammer trnascan trnascan-stats populate-tax remove-job
single_genome: convert_to_fasta trnascan trnascan-stats genomeatlas populate-tax remove-job

populate_tax: populate-tax remove-job

convert_to_fasta: $(ACCESSION)_$(VERSION).fna

$(ACCESSION)_$(VERSION).fna:
	@echo "Converting to fasta"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=convert_to_fasta --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell python $(BIN_DIR)convert_gbk_fasta.py -i $(GENOME_DIR)$(ACCESSION)_$(VERSION).gbk -o $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna > /dev/null; if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

rnammer: $(ACCESSION)_$(VERSION).rrna

$(ACCESSION)_$(VERSION).rrna: $(ACCESSION)_$(VERSION).fna
	@echo "Running rnammer"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=rnammer --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell /home/people/helen/rnammer-1.2/rnammer -S bac -m ssu -f $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).rrna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna; if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

#UNCOMMENT the dependency when rnammer is running on cge
#rnammer-stats: $(ACCESSION)_$(VERSION).rrna2  
rnammer-stats: 
	@echo "Running stats for rnammer"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=rnammer-stats --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell python $(BIN_DIR)rnammer_stats.py $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).rrna2 > /dev/null; if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

trnascan: $(ACCESSION)_$(VERSION).trna

$(ACCESSION)_$(VERSION).trna: $(ACCESSION)_$(VERSION).fna
	@echo "Running trnascan"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=trnascan --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell /home/people/helen/bin/trnascan-1.4 -o $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).trna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna > /dev/null 2> /dev/null; if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

trnascan-stats: $(ACCESSION)_$(VERSION).trna
	@echo "Running trnascan-stats"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=trnascan-stats --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell python $(BIN_DIR)trnascan_stats.py $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).trna; if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

genomeatlas: $(ACCESSION)_$(VERSION).genomeatlas.ps

$(ACCESSION)_$(VERSION).genomeatlas.ps: $(ACCESSION)_$(VERSION).atlas.cfg
	@echo "Running Genome Atlas"
        $(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=genomeatlas-run --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell $(BIN_DIR)/atlas -f $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).gbk -c $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).atlas.cfg -o $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).genomeatlas.ps  > /dev/null 2> /dev/null; if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

$(ACCESSION)_$(VERSION).atlas.cfg:
	@echo "Generating atlas CFG file
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=genomeatlas-config --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell $(BIN_DIR)/atlas_createConfig -ref $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).gbk > /dev/null 2> /dev/null; if [ $$? -eq 0 ]; then python $(BIN_DIR)dblog.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); if)

populate-tax:
	@echo "Running populate-tax"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=populate-tax --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell python $(BIN_DIR)populate_tax.py $(ACCESSION) $(VERSION); if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)
	
remove-job:
	@echo "Removing job from db"
	$(eval LOGID=$(shell python $(BIN_DIR)db_log.py --start --job=$(JOB) --job_uuid=$(JOB_UUID) --jobstep=remove-job --accession=$(ACCESSION) --version=$(VERSION)))
	$(shell python $(BIN_DIR)db_log.py --remove_job --job_uuid=$(JOB_UUID); if [ $$? -eq 0 ]; then python $(BIN_DIR)db_log.py --finish=success --logid=$(LOGID); else python $(BIN_DIR)db_log.py --finish=failure --logid=$(LOGID); fi)

clean:
	rm $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).fna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).rrna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).trna $(OUTPUT_DIR)$(ACCESSION)_$(VERSION).genomeatlas.ps

