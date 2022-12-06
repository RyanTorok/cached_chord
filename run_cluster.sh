donefile="done.txt"
nodes=5
writes=100
reads=1000
hosts=(433 418 426 428 431);
sshflags="-oStrictHostKeyChecking=no"
for dist in zipf uniform; do
    for cache in lru none fifo mru lfu lifo; do
        for size in 10 20 50 100 200; do
            master="pc${hosts[0]}.emulab.net";
            ssh $sshflags $master "touch $donefile";
            master_ip=$(ssh $master 'hostname -I' | awk '{print $1}');
            pids=();
            echo "Starting experiment with Cache = $cache($size), Dist = $dist"
            for node in $( seq 0 $(($nodes - 1))); do
                if [ $node == 0 ]; then
                    node_id=0;
                else
                    node_id=$((($RANDOM + 1) * ($RANDOM + 1) * ($RANDOM % 4 + 1) - 1));
                fi
                cmd="killall chord; timeout 400 ~/chord -n $node_id --keys $writes --cache $cache --cache-size $size --master-ip $master_ip --requests $reads --distribution $dist --index $node --total $nodes";
                ssh $sshflags "pc${hosts[$node]}.emulab.net" "$cmd" &
                pids+=($!);
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
                alive=0
                for pid in "${pids[@]}"; do
                    if ps -p $pid > /dev/null; then
                        alive=1
                        break;
                    fi
                done;
                if [ $alive == 0 ]; then
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
