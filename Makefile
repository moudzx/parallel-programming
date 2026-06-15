CC     = mpicc
CFLAGS = -O2 -fopenmp -Wall -Wextra
NP     = 8

DIR  := $(word 1,$(MAKECMDGOALS))
FILE := $(word 2,$(MAKECMDGOALS))

.PHONY: clean run $(DIR) $(FILE)

.DEFAULT_GOAL := _noargs

_noargs:
	@echo "Usage: make <directory_to_search> <file_to_search>"

$(DIR): search
	@mpiexec -n $(NP) --oversubscribe ./search "$(DIR)" "$(FILE)"

$(FILE):
	@:

search: main.c
	$(CC) $(CFLAGS) main.c -o search -lpthread

clean:
	rm -f search

%:
	@:
