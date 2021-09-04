for algo in "parallel" "concurrent"
do
    for hashbits in {1..18}
    do 
        for threads in "1" "2" "4" "8" "16" "32"
        do
            echo "${algo} 24 ${hashbits} ${threads} 1 (10 times)"
            for iter in {1..10}
            do
                cargo run --release $algo 24 $hashbits $threads 1
            done
        done
    done
done
