raw = open("raw_stats.csv", "r")
out = open("stats.csv", "w")
raw_lines = raw.readlines()
stats = {}
key = None
ovr_dist = 'uniform'
for line in raw_lines:
    line = line[1:-1]
    try:
        (node_id, latency, n_reads, cache_type, cache_size, distribution) = line.split(", ")
        if distribution == "zipf(1.2)":
            ovr_dist = "zipf(1.2)"
        key = (n_reads, cache_type, cache_size, ovr_dist)
        if key not in stats:
            stats[key] = (float(latency), 1)
        else:
            (total, count) = stats[key]
            stats[key] = (total + float(latency), count + 1)
    except:
        if key is not None:
            spl = line.split(", ");
            for i in range(len(spl)):
                if i == 0:
                    continue
                try:
                    if spl[i].contains("."):
                        print(spl[i])
                    f = float(string.strip(spl[i]))
                    print("found float = " + str(f));
                    (total, count) = stats[key]
                    stats[key] = (total + f, count + 1)
                except:
                    pass
for ((n_reads, cache_type, cache_size, distribution), (total, count)) in stats.items():
    out.write("<{}, {}, {}, {}, {}>\n".format(str(total / count), str(n_reads), str(cache_type), str(cache_size), str(distribution)))
