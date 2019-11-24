compiler: lex.l grammar.y main.cpp def.cpp utility.cpp ptp.cpp
	bison -d grammar.y
	flex lex.l
	clang++ -w -std=c++14 -DSTAT_FILE='"$F"' grammar.tab.c lex.yy.c def.cpp utility.cpp ptp.cpp main.cpp -o s3qlCompiler
	@[ "${D}" ] || ( echo ">> Query file path D is not set!"; exit 1 )
	@[ "${O}" ] || ( echo ">> Output file path O is not set!"; exit 1 )
	    @for f in $(shell ls ${D}); do \
		echo $${f}; \
		./s3qlCompiler < ${D}/$${f} $${f} $${O}; \
	    done
	rm grammar.tab.c lex.yy.c 
 
