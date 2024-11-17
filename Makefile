CC=g++
FLAGS= -std=c++17 -Wall -Wextra -Wshadow
BIN=bin
SRC = src

main:
	mkdir -p ${BIN}
	${CC} ${FLAGS} -I src/ ${SRC}/main.cpp -o ${BIN}/main

clear:
	rm -f bin/*

run:
	./${BIN}/main