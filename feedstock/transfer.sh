#!/bin/bash

# Script to transfer files from Zenodo to OSN
for year in {2002..2020}
do
  url="http://globalchange.bnu.edu.cn/laiv061/global_lai_0.05/lai_8-day_0.05_${year}.nc"


  # Run the rclone command for each URL
  rclone copyurl "$url" osnmanual:leap-pangeo-manual/CASM \
    --auto-filename -vv --progress --fast-list --max-backlog 500000 \
    --s3-chunk-size 200M --s3-upload-concurrency 128 \
    --transfers 128 --checkers 128
done
