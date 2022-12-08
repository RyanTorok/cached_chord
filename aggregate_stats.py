raw = open("raw_stats.csv", "r")
out = open("stats.csv", "w")
raw_lines = raw.readlines()
stats = {}
for line in raw_lines:
    line = line[1:-1]
    (node_id, latency, n_reads, cache_type, cache_size, distribution) = line.split(",")
    key = (n_reads, cache_type, cache_size, distribution)
    if key not in stats:
        stats[key] = (float(latency), 1)
    else:
        (total, count) = stats[key]
        stats[key] = (total + float(latency), count + 1)
for ((n_reads, cache_type, cache_size, distribution), (total, count)) in stats.items():
    out.write("<{}, {}, {}, {}, {}>\n".format(str(total / count), str(n_reads), str(cache_type), str(cache_size), str(distribution)))
