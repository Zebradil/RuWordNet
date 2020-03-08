DB_HOST ?= localhost
DB_USER ?= ruwordnet
DB_PASS ?= ruwordnet
DB_NAME ?= ruwordnet

define runsql
	PGPASSWORD=$(DB_PASS) psql -h $(DB_HOST) -U $(DB_USER) $(DB_NAME) < $(1)
endef

define runquery
	PGPASSWORD=$(DB_PASS) psql -h $(DB_HOST) -U $(DB_USER) $(DB_NAME) -c $(1) -t > $(2)
endef


# RAW -> XML
gen-ruthes: gen-ruthes-concepts gen-ruthes-textentries gen-ruthes-synonyms gen-ruthes-relations

gen-ruthes-concepts:
	pipenv run python raw2xml/concepts.py

gen-ruthes-textentries:
	pipenv run python raw2xml/text_entries.py

gen-ruthes-synonyms:
	pipenv run python raw2xml/synonyms.py

gen-ruthes-relations:
	pipenv run python raw2xml/relations.py


# XML -> SQL
load-ruthes-to-db:
	pipenv run python xml2sql/xml2sql.py


# IMPORT RUTHES RELATIONS
import-extra-relations: import-antonyms import-class-instance import-meronymy import-domains

import-antonyms:
	pipenv run python scripts/import_antonyms.py \
		-s data/antonyms_final.txt

import-class-instance:
	pipenv run python scripts/import_class-instance_relations.py \
		-s data/class-instance_edited.txt

import-meronymy:
	pipenv run python scripts/import_meronymy_relations.py \
		-s data/add_part.txt \
		--type ЦЕЛОЕ \
		--sub-type add_part
	pipenv run python scripts/import_meronymy_relations.py \
		-s data/process_steps_final.txt \
		--type ЧАСТЬ \
		--sub-type process_steps
	pipenv run python scripts/import_meronymy_relations.py \
		-s data/classical_meronymy_edited.txt \
		--type ЧАСТЬ  \
		--sub-type classical_meronymy

import-domains:
	pipenv run python scripts/import_domains.py

fix-ruthes-relations:
	$(call runsql,'scripts/sql/fixup_relations.sql')


# GENERATE RUWORDNET
gen-ruwordnet:
	pipenv run python sql2sql/sql2sql.py
	$(call runsql,'sql/post-conversion.sql')

gen-ruwordnet-xml:
	rm -f sql2xml/out/rwn/*
	pipenv run python sql2xml/sql2rwn_xml.py
	cd sql2xml/out/rwn; tar -czvf ../rwn-$$(date +%F).tgz .


# IMPORT RUWORDNET RELATIONS
import-cause-entailment:
	pipenv run python scripts/import_cause-entailment.py \
		-s data/cause.utf8.filtered.txt \
		--name cause
	pipenv run python scripts/import_cause-entailment.py \
		-s data/entailment.utf8.filtered.txt \
		--name entailment

# GENERATE RUWORDNET RELATIONS
gen-ruwordnet-relations: gen-derived-from gen-composed-of

gen-derived-from:
	pipenv run python scripts/cognates_relation_statistics.py

gen-composed-of:
	pipenv run python scripts/collocation_relation_statistics.py


# UPDATE WEBSITE
dump-db:
	$(eval DATE := $(shell date +%F))
	PGPASSWORD=$(DB_PASS) pg_dump -h $(DB_HOST) -U $(DB_USER) $(DB_NAME) -n public > rwn-$(DATE).sql
	tar -czvf rwn-$(DATE).sql.tgz rwn-$(DATE).sql

# IMPORT ILI DATA
import-syn-tabs:
	scripts/import_ili_data.sh

# GENERATE OMW RWN
gen-ruwordnet-omw:
	pipenv run python sql2xml/sql2rwn_omw_xml.py > rwn_omw.xml
	tar -czvf rwn_omw-$$(date +%F).tgz rwn_omw.xml

#####################################################33
# MISC
######

comma:= , # This is needed to avoid splitting query into arguments
export-roots-data:
	$(call runquery,"SELECT word$(comma) ARRAY_AGG(root ORDER BY root) FROM roots WHERE quality='louk' GROUP BY word ORDER BY 2",'louk.roots_per_word.txt')
	$(call runquery,"SELECT root$(comma) ARRAY_AGG(word) FROM roots WHERE quality='louk' GROUP BY root",'louk.words_per_root.txt')

