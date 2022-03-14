

.PHONY: lookup
lookup:
	@printf 'Producing NSPL lookup table...'
	@python -m src.lookup_table

