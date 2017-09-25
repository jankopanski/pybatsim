
EXPE_FOLDERS := $(wildcard tests/*)
SIMULS_OUT := $(patsubst %,%/out_jobs.csv,$(EXPE_FOLDERS))
GANTTS_OUT := $(patsubst %,%/gantt.png,$(EXPE_FOLDERS))


all: simuls

simuls: testIntervals $(SIMULS_OUT) coverage_html
	@if [ "$(SIMULS_OUT)" == "" ]; then \
		echo; \
		echo "All experiments were generated."; \
		echo " -> You may have to run the command again to run the experiments."; \
	fi

coverage_html: ../.coverage
	cd ..; coverage html -d tests/coverage_html

testIntervals: generate
	cd ..; python -m coverage run -a schedulers/intervalContainer.py

%/out_jobs.csv:
	cd ..; python launch_expe.py --quiet tests/$*/expe.json || (rm -f $@; exit 1)

gantts: $(GANTTS_OUT)

gantts_okular: $(GANTTS_OUT)
	okular $^

%/gantt.png: %/out_jobs.csv
	python ../../../../evalys/evalys/gantt.py $* $*/gantt.png

generate:
	python generate.py

clean:
	rm tests/*/out_jobs.csv
	cd ..; python -m coverage erase
