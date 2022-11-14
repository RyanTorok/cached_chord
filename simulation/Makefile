all:
	cargo build --release
	
run:
	cargo run --release -- -n 5 --keys 10000 --cache none --cache-size 256 --requests 100 --distribution uniform -v
	
clean:
	rm -r target

clearlogs:
	rm -r logs/*
