.PHONY: e1 e2 e21 e22

two_%_i32s:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 -g [$*]i32 > $@

one_%_i32s:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 > $@

i32_%_bools:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 -g [$*]bool  > $@

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


e22_%:  $(SIZES:%i32_%_bools) e22.fut
	futhark bench --backend=$* \
	--json=e22_$*.json \
	e22.fut 

e21_%:  $(SIZES:%i32_%_bools) e21.fut
	futhark bench --backend=$* \
	--json=e21_$*.json \
	e21.fut 


eb22_%:  $(SIZES:%one_%_i32s) eb22.fut
	futhark bench --backend=$* \
	--json=eb22_$*.json \
	eb22.fut 

eb21_%:  $(SIZES:%one_%_i32s) eb21.fut
	futhark bench --backend=$* \
	--json=eb21_$*.json \
	eb21.fut 

e21:
	make e21_c; \
	make e21_multicore; \

	make eb21_c; \
	make eb21_multicore; \

e22:
	make e22_c; \
	make e22_multicore; \

	make eb22_c; \
	make eb22_multicore;\

e2:
	make e21; \
	make e22; \

	


g:
	jupyter notebook

