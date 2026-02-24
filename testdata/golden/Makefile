CC = cc
CFLAGS = -Wall -O2
PROG = gen_golden

.PHONY: all clean generate

all: generate

$(PROG): gen_golden.c
	$(CC) $(CFLAGS) -o $(PROG) gen_golden.c

generate: $(PROG)
	./$(PROG)

clean:
	rm -f $(PROG) *.dir *.pag
