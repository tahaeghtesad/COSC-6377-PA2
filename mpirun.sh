#!/bin/bash

for j in 2 4 8 16 32 64 128 256
  do
    mytime="$(TIMEFORMAT='%3R'; time (mpirun --mca btl_vader_backing_directory /tmp -np 2 ./a.out graphs/CA-AstroPh.txt) 2>/dev/ 1>/dev/null)"
#            echo $n,$j,$mytime >> plot.csv
    printf "$mytime,"
done