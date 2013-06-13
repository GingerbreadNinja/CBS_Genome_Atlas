#!/usr/bin/csh

if ( `showq -u helen | grep "Total jobs" | awk '{print $3}'` <= 50 ) then
    python /home/people/helen/CBS_Genome_Atlas/analysis/submit_jobs_to_queue.py --missing
else
    echo "doing nothing"
endif
