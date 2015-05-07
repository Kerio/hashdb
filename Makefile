.PHONY: all clean

all:
#	make -C utils
#	make -C helpers
#	make -C db
	make -C thirdparty 
#	make -C tool
#	make -C testUtils
#	make -C tests
#	make -C benchmark

clean:
#	make -C utils clean
#	make -C helpers clean
#	make -C db clean
	make -C thirdparty clean
#	make -C tool clean
#	make -C testUtils clean
#	make -C tests clean
#	make -C benchmark clean
