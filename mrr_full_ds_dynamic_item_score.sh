#!/bin/sh

export PATH=$PATH:/usr/bin/python3.6:/usr/bin/python3:/usr/bin/python:/usr/local/bin:/usr/bin/java:/home/ec2-user/.local/bin:/home/ec2-user/bin:/usr/share/Modules/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin

cd /home/ec2-user/scripts/Talend_2_python/Step_1_Source_To_Mirror_Part_16

python  /home/ec2-user/scripts/Talend_2_python/Step_1_Source_To_Mirror_Part_16/ds_model/mrr_full_ds_dynamic_item_score.py --model "018"

exit 0
