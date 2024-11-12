CC=g++
FLAGS= -std=c++17 -Wall -Wextra -Wshadow
BIN=bin

main:
	mkdir -p ${BIN}
	${CC} ${FLAGS} main.cpp -o ${BIN}/main

clear:
	rm -f bin/*

run:
	./${BIN}/main