.PHONY: e1 e2 e21 e22 e23 e3 e3h e3w e3hw e3n

two_%_i32s:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 -g [$*]i32 > $@

one_%_i32s:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 > $@

i32_%_bools:
	futhark dataset -b --i32-bounds=-10000:10000 -g [$*]i32 -g [$*]bool > $@

one_%_two:
	futhark dataset -b --i32-bounds=-10000:10000 --i64-bounds=-10:$* -g [$*]i32 -g [$*]i64 -g [$*]i32 > $@

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

e23_%:  $(SIZES:%one_%_two) e23.fut
	futhark bench --backend=$* \
	--json=e23_$*.json \
	e23.fut 

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

eb23_%: $(SIZES:%one_%_two) eb23.fut
	futhark bench --backend=$* \
	--json=eb23_$*.json \
	eb23.fut

e3h_%: e3h.fut
	futhark bench --backend=$* \
	--json=e3h_$*.json \
	e3h.fut

e3w_%: e3w.fut
	futhark bench --backend=$* \
	--json=e3w_$*.json \
	e3w.fut

e3hw_%: e3hw.fut
	futhark bench --backend=$* \
	--json=e3hw_$*.json \
	e3hw.fut

e3n_%: e3n.fut
	futhark bench --backend=$* \
	--json=e3n_$*.json \
	e3n.fut

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

e23:
	make e23_c; \
	make e23_multicore; \

	make eb23_c; \
	make eb23_multicore;\

e2:
	make e21; \
	make e22; \
	make e23; \
	
e3h:
	make e3h_c; \
	make e3h_multicore; \

e3w:
	make e3w_c; \
	make e3w_multicore; \

e3hw:
	make e3hw_c; \
	make e3hw_multicore; \

e3n:
	make e3n_c; \
	make e3n_multicore; \

e3:
	make e3h; \
	make e3w; \
	make e3hw; \
	make e3n; \

g:
	jupyter notebook

