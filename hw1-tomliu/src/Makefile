CC=g++
CFLAGS=-ggdb -std=c++11 -Wall -pedantic

H_FILES=CalcFramer.hpp CalcParser.hpp
FILES=CalcServer.c DieWithError.c HandleTCPClient.c CalcFramer.cpp CalcParser.cpp

all: calcserver

calcserver: $(FILES) $(H_FILES)
	$(CC) $(CFLAGS) -o calcserver $(FILES)

clean:
	rm -rf calcserver
