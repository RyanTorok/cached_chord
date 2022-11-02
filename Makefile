all:
	cargo +nightly build --release
	
clean:
	rm -rf target
	
run:
	cargo +nightly run --release
