all:
	cargo build --release
	
run:
	cargo run --release -- -n 100 --keys 0 --cache none --cache-size 256 --requests 0 --distribution uniform -v
	
clean:
	rm -r target

clearlogs:
	rm -r logs/*
