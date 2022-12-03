all:
	cargo build --release
	
run:
	cargo run --release -- -n 5 --keys 100 --cache none --cache-size 256 --requests 1000 --distribution uniform 
	
clean:
	rm -r target

clearlogs:
	rm -r logs/*
