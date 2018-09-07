# Index var cov calculation
Index.var.cov.calculation <- function(index_dt_final, process_period, lambda.var.list, lambda.cov.list){
  # index_dt_final = selected_index_dt_final
  # lambda.var.list = c(0.97,0.96)
  data = as.data.table(index_dt_final)
  name_list = unique(index_dt_final$SEC_NAME)
  A_share_indexes_length = length(c(basic.index,sw.industry.indexes))
  # Name as factor
  data$SEC_NAME = as.factor(data$SEC_NAME)
  all_indexes_length = length(name_list)
  # Needed parameter in calculation
  # data$logreturn_T0 = 0
  data$RVar_T1 = 0
  cov_df = data.frame(matrix(numeric(0),nrow=c(process_period+1)))
  name_list = as.character(unique(data$SEC_NAME))
  length_name = length(name_list)
  
  # Calculation
  
  for (j in (1:length_name)){
    # j = 29
    if ( j != length_name){
      lambda = lambda.var.list[1]
    } else {
      lambda = lambda.var.list[2]
    }
    data_temp = data[data$SEC_NAME %in% name_list[j],]
    ## Calculate the first day variance in process period
    day1_ewma = EWMA(review_period,lambda,data_temp$logreturn_T0)
    data_temp$RVar_T1[review_period] = day1_ewma
    
    ## Calculate all covariances in the process period
    if (j > 1 & j <= A_share_indexes_length){
      for (m in (c(j-1):1)){
      # j = 29
        # m = 1
        cov_df[paste("cov",j,m,sep ="")] = 0
        cov_df[paste("cov",j,m,sep ="")][1,] = Covar(review_period, lambda, data_temp$logreturn_T0,data[data$SEC_NAME %in% name_list[m],]$logreturn_T0)
        
        for (z in (2:c(total_days-review_period+1))){
          cov_df[paste("cov",j,m,sep ="")][z,] = lambda*cov_df[paste("cov",j,m,sep ="")][c(z-1),]+(1-lambda)*(data_temp$logreturn_T0[z]*(data[data$SEC_NAME %in% name_list[m],]$logreturn_T0[z]))
          # length(lambda.cov.list)
        }
      }
    } else if (j > c(A_share_indexes_length)){
      for (m in (c(j-1):1)){
      # m = 1
        cov_df[paste("cov",j,m,sep ="")] = 0
        cov_df[paste("cov",j,m,sep ="")][1,] = Covar(review_period, lambda.cov.list, data_temp$logreturn_T0,data[data$SEC_NAME %in% name_list[m],]$logreturn_T0)
        for (z in (2:c(total_days-review_period+1))){
          cov_df[paste("cov",j,m,sep ="")][z,] = lambda.cov.list*cov_df[paste("cov",j,m,sep ="")][c(z-1),]+(1-lambda.cov.list)*(data_temp$logreturn_T0[z]*(data[data$SEC_NAME %in% name_list[m],]$logreturn_T0[z]))
        }
      }
    }
    
    # Use the recursive formula to calculate variance every day
    for (z in (process_start_day:total_days)){
      # data_temp$logreturn_T0[z] = log(data_temp$CLOSE[z]/data_temp$CLOSE[z-1])
      data_temp$RVar_T1[z] = lambda*data_temp$RVar_T1[z-1] +(1-lambda)*(data_temp$logreturn_T0[z]**2)
    }
    data[data$SEC_NAME %in% name_list[j],] = data_temp
  }
  full_data <- data
  final_data <- data[, .SD[process_start_day:total_days],by = SEC_NAME]
  # cov_df <- cov_df
  
  left_length = process_period + 1
  # Construct the correlation matrix
  cor.mat = matrix(0,all_indexes_length, all_indexes_length)
  cor.mat[1,1] = 1
  for (i in (2:length_name)){
    cor.mat[i,i] = 1
      for (j in (c(i-1):1)){
        cor.mat[i,j] = cor.mat[j,i] = cov_df[paste("cov",i,j,sep ="")][left_length,]/(sqrt(cov.list[i])*sqrt(cov.list[j]))
      }
  }
  
  cov.mat = t(t(sqrt(cov.list)))%*%sqrt(cov.list)*cor.mat
  # colnames(cov.mat) = name_list
  # rownames(cov.mat) = name_list
  variance.list <- final_data[, .SD[c(total_days-review_period)], by = SEC_NAME]$RVar_T1
  final_list = list(full_data = full_data, final_data = final_data, cov_df = cov_df, variance.list = variance.list, cov.mat = cov.mat)
  return(final_list)
}
# Index.var.cov.calculation(index_dt_final, process_period, name_list)