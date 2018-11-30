# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= CHALLENGES Q2 ===========================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Challenges Q2 Investigate Quality Flag ==============================
#fill all NA value for element to No Flag so that it could use group by 
check_all_daily = check_all_daily.fillna('No Flag', subset=['quality_flag'])

#group by quality flag to check on the counts for all daily
quality_check_all_daily = check_all_daily.groupBy("quality_flag").agg(F.count(check_all_daily.quality_flag).alias("count")).orderBy("count",ascending=False)
quality_check_all_daily.cache()
quality_check_all_daily.show(70)
quality_check_all_daily.filter(quality_check_all_daily.quality_flag ~= 'No Flag').count()
