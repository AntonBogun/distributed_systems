CC=clang++
FLAGS= -std=c++17 -Wall -Wextra -Wshadow
BIN=bin

main:
	${CC} ${FLAGS} main.cpp -o ${BIN}/main

clear:
	rm -f bin/*