donefile="done.txt"
nodes=5
writes=100
reads=1000
for dist in zipf uniform; do
    for cache in lru none fifo mru mfu lifo; do
        for size in 10 20 50 100 200; do
            master=node0.cached-chord.cos518f22.emulab.net;
            ssh $master "touch $donefile";
            master_ip=$(ssh $master 'hostname -I' | awk '{print $1}');
            index=0;
            pids=();
            for node in $( seq 0 $nodes ) do
                if [ $node == 0 ]; then
                    node_id=0;
                else
                    node_id=$((($RANDOM + 1) * ($RANDOM + 1) * ($RANDOM % 4 + 1) - 1));
                fi
                cmd="timeout 600 ~/chord -n $node_id --keys $writes --cache $cache --master-ip $master_ip --requests $reads --distribution $dist --index $index --total $nodes";
                ssh node${node}.cached-chord.cos518f22.emulab.net "$cmd" &
                pids+=($!);
                index=$((index + 1));
            done;
            while true; do
                sleep 5;
                scp $master:$donefile $donefile;
                num_done=$(wc -l < $donefile);
                if [ $num_done == $nodes ]; then
                    for pid in $pids; do
                        kill $pid;
                    done;
                    break;
                fi
            done;
            ssh $master "rm $donefile";
            rm $donefile;
            if [ $cache == "none" ]; then
                if [ $size == 10 ]; then
                    break;
                fi
            fi
        done;
    done;
done;
