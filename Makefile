all:
	cargo +nightly build --release

clean:
	rm -rf target
	rm -f report.html

run:
	cargo +nightly run --release

report:
	markdown report.md > report.html
