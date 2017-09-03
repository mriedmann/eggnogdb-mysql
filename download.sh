#!/bin/sh

mkdir -p files
(
cd files
wget -qO- http://eggnogdb.embl.de/download/eggnog_4.5/eggnog4.functional_categories.txt > eggnog4.functional_categories.txt
wget -qO- http://eggnogdb.embl.de/download/eggnog_4.5/eggnog4.species_list.txt > eggnog4.species_list.txt
wget -qO- http://eggnogdb.embl.de/download/eggnog_4.5/data/meNOG/meNOG.annotations.tsv.gz | gzip -d > meNOG.annotations.tsv
wget -qO- http://eggnogdb.embl.de/download/eggnog_4.5/data/meNOG/meNOG.members.tsv.gz | gzip -d > meNOG.members.tsv
)