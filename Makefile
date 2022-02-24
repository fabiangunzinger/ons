

.PHONY: lookup
lookup:
	@printf 'Producing NSPL lookup table...'
	@python -m ons.lookup_table

