two_%_i32s:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 -g [$*]i32 > $@

SIZES = 100 1000 10000 100000 1000000 5000000 10000000

e11_%: $(SIZES:%two_%_i32s) e11.fut
	futhark bench --backend=$* \
	--json=e11_$*.json \
	e11.fut

e13_%: $(SIZES:%two_%_i32s) e13.fut
	futhark bench --backend=$* \
	--json=e13_$*.json \
	e13.fut

e1:
	make e11_c; \
	make e11_multicore; \

	make e13_c; \
	make e13_multicore; \

g:
	jupyter notebook

