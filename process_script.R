# if (length(date.list) < 570){ # date.list
#   process_period = ceiling(length(date.list)/2) # date.list
#   review_period =floor(length(date.list)/2) # date.list
# } else {
#   process_period = 285
#   review_period = 285
# }


process_period = 285
review_period = 285
total_days = process_period + review_period 
# data_end_day = as.Date(date.list[length(date.list)]) # date.list
data_end_day = date.list[length(date.list)]
data_start_day = date.list[length(date.list)-total_days+1]
# Other Time parameters needed
process_start_day = review_period + 1

temp_result = Lambda.optimize(process_period, selected_index_dt_final)
lambda.var.list = temp_result$lambda.var.list
lambda.cov.list = temp_result$lambda.cov.list

temp_list = Index.var.cov.calculation(selected_index_dt_final, process_period, lambda.var.list, lambda.cov.list)
full_data = temp_list$full_data
setorder(full_data, seccode, date)
final_data = temp_list$final_data
cov_df = temp_list$cov_df
cov.list = temp_list$variance.list
cov.mat = temp_list$cov.mat
cov.series = as.vector(cov.mat)
# matrix(as.vector(cov.series),ncol = 30)
