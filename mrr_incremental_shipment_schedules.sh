#!/bin/sh

#  mrr_incremental_shipment_schedules

export PATH=$PATH:/usr/bin/python3.6:/usr/bin/python3:/usr/bin/python:/usr/local/bin:/usr/bin/java:/home/ec2-user/.local/bin:/home/ec2-user/bin:/usr/share/Modules/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin

cd /home/ec2-user/scripts/Talend_2_python/Step_1_Source_To_Mirror_Part_16/increment

day=$(date +"%u")

if ((day == 6)); then
   echo "WEEKEND"
   python /home/ec2-user/scripts/Talend_2_python/Step_1_Source_To_Mirror_Part_16/increment/mrr_incremental_shipment_schedules.py --is_full '1'
else
   echo "WORKING DAY"
   python /home/ec2-user/scripts/Talend_2_python/Step_1_Source_To_Mirror_Part_16/increment/mrr_incremental_shipment_schedules.py --is_full '0'
fi

exit 0
