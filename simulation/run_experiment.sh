donefile="done.txt"
nodes=5
writes=100
reads=1000
for dist in uniform zipf; do
    for cache in none lru fifo mru lfu lifo; do
        for size in 10 20 50 100 200; do
            touch $donefile
            #echo "$dist $cache $size"
            killall chord;
            timeout 500 cargo run --release -- -n $nodes --keys $writes --cache $cache --cache-size $size --requests $reads --distribution $dist --rtt 60 &
            pid=$!
            while true; do
                sleep 5;
                num_done=$(wc -l < $donefile)
                if [ $num_done == $nodes ]; then
                    kill $pid
                    rm -r logs/*
                    break;
                fi
                if ps -p $pid > /dev/null; then
                    dummy=0                    
                else
                    break;
                fi
            done;
            rm $donefile
            if [ $cache == "none" ]; then
                if [ $size == 10 ]; then
                    break;
                fi
            fi
        done;
    done;
done;
