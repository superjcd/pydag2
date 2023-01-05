from pydag.utils import get_cron_for_job

crons = """
# Begin Plan generated jobs for: main
0 0 1 1,3,5,7,9,11 * pwd
0 0 * * 6,0 date
# End Plan generated jobs for: main

"""

cron = get_cron_for_job("main", crons)

print(cron)
