Lambda.optimize <- function(process_period, index_dt_final){
  ### Period selection
  # index_dt_final1 = index_dt_final
  # index_dt_final = selected_index_dt_final
  # index_dt_final = selected_index_dt_final[selected_index_dt_final$SEC_NAME %in% name_list,]
  # length_name = length(name_list)
  ### loop parameter input
  lambda_data = index_dt_final[index_dt_final$seccode %in% c(basic.index,hs.index),]
  lambda_data = as.data.table(lambda_data)
  ###
  
  # Needed parameter in calculation
  # lambda_data$logreturn_T0 = 0
  lambda_data$RVar_T1 = 0
  # cov_df = data.frame(matrix(numeric(0),nrow=c(total_days-review_period+1)))
  name_list <- unique(lambda_data$SEC_NAME)
  length_name <- length(unique(lambda_data$seccode))
  A_share_indexes_length = length(name_list) # Here, we only have 2 indexes, for the convenience, prefer not to change the variable name here.
  ### parameter input
  RMSE.period = process_period-1
  ###
  lambda.range <- seq(0.90, 0.999, by = 0.01)
  RMSE.return = data.frame(matrix(numeric(0),nrow = RMSE.period))
  RMSE.var = data.frame(matrix(numeric(0),nrow = RMSE.period))
  RMSE = data.frame(matrix(numeric(0),nrow = length(lambda.range)))
  RMSE.cov = data.frame(matrix(numeric(0),nrow = length(lambda.range)))
  temp_coreturn_df = data.frame(matrix(numeric(0),nrow=c(RMSE.period)))
  temp_cov_df = data.frame(matrix(numeric(0),nrow=c(process_period+1)))
  for (l in (1:length(lambda.range))){
    # l = 2
    # Calculation
    for (j in (1:length_name)){
      # j = 1
      data_temp = lambda_data[lambda_data$SEC_NAME %in% name_list[j],]
      ## Calculate the variance of the "first day"
      day1_ewma = EWMA(review_period,lambda.range[l],data_temp$logreturn_T0)
      data_temp$RVar_T1[review_period] = day1_ewma
      
      for (d in (process_start_day:total_days)){
        # data_temp$logreturn_T0[d] = log(data_temp$CLOSE[d]/data_temp$CLOSE[d-1])
        data_temp$RVar_T1[d] = lambda.range[l]*data_temp$RVar_T1[d-1] +(1-lambda.range[l])*(data_temp$logreturn_T0[d]**2)
      }
      lambda_data[lambda_data$SEC_NAME %in% name_list[j],] = data_temp
      
      # Calculate the covariance
      if (j > 1 & j <= A_share_indexes_length){
        m = 1
        # print (m)
        temp_cov_df[paste("cov",j,m,sep ="")] = 0
        temp_cov_df[paste("cov",j,m,sep ="")][1,] = Covar(review_period, lambda.range[l], data_temp$logreturn_T0,lambda_data[lambda_data$SEC_NAME %in% name_list[m],]$logreturn_T0)
        for (z in (2:c(process_period+1))){
          temp_cov_df[paste("cov",j,m,sep ="")][z,] =(lambda.range[l]*temp_cov_df[paste("cov",j,m,sep="")][c(z-1),])+((1-lambda.range[l])*(data_temp$logreturn_T0[z+process_start_day-2]*(lambda_data[lambda_data$SEC_NAME %in% name_list[m],]$logreturn_T0[z+process_start_day-2])))
        }
      } else if(j > c(A_share_indexes_length+1)){
        for (m in (c(j-1):c(A_share_indexes_length+1))){
          # print (m)
          temp_cov_df[paste("cov",j,m,sep ="")] = 0
          temp_cov_df[paste("cov",j,m,sep ="")][1,] = Covar(review_period, lambda.range[l], data_temp$logreturn_T0,lambda_data[lambda_data$SEC_NAME %in% name_list[m],]$logreturn_T0)
          for (z in (2:c(process_period+1))){
            temp_cov_df[paste("cov",j,m,sep ="")][z,] =(lambda.range[l]*temp_cov_df[paste("cov",j,m,sep="")][c(z-1),])+((1-lambda.range[l])*(data_temp$logreturn_T0[z+process_start_day-2]*(lambda_data[lambda_data$SEC_NAME %in% name_list[m],]$logreturn_T0[z+process_start_day-2])))
          }
        }
      }
    }
    
    final_data_temp = lambda_data[, .SD[process_start_day:total_days],by = SEC_NAME]
    index.return.list = final_data_temp[, .SD[2:process_period], by = SEC_NAME]$logreturn_T0 # Consider t0 forecast
    index.var.list = final_data_temp[, .SD[1:c(process_period-1)], by = SEC_NAME]$RVar_T1 # Consider time t forecast rather than time t+1
    final_cov_df = temp_cov_df[2:process_period,] # 187 dates include t0 and tT+1, we need t1 to tT
    # Calculate co-return to calculate RMSE
    for (a in (1:length(name_list))){ # 赋值次数太�?
      RMSE.return[name_list[a]] = index.return.list[c(1+RMSE.period*(a-1)):c(RMSE.period*a)]
      if (a > 1 & a <= A_share_indexes_length){
        # for (n in (c(a-1):1)){
        n = 1
        temp_coreturn_df[paste("return",a,n,sep="")] = RMSE.return[name_list[a]] * RMSE.return[name_list[n]]
        # }
      } else if (a > c(A_share_indexes_length+1)){
        for (n in (c(a-1):c(A_share_indexes_length+1))){
          temp_coreturn_df[paste("return",a,n,sep="")] = RMSE.return[name_list[a]] * RMSE.return[name_list[n]]
        }
      }
      RMSE.var[name_list[a]] = index.var.list[c(1+RMSE.period*(a-1)):c(RMSE.period*a)]
      error = sqrt(sum((RMSE.return[,a]**2 - RMSE.var[,a])**2)/RMSE.period)
      # plot(RMSE.return[,a]**2)
      # lines(RMSE.var[,a])
      RMSE[l,a] = error # "l" states different lambda, "a" states different indexes
    }
    if (length(colnames(temp_coreturn_df))>0){
      for (c in (1:length(colnames(temp_coreturn_df)))){
        RMSE.cov[l,c] = sqrt(sum((temp_coreturn_df[,c] - as.matrix(final_cov_df)[,c])**2)/RMSE.period)
      }
    }
    
  }
  colnames(RMSE) = name_list
  colnames(RMSE.cov) = colnames(temp_coreturn_df) # !!!!!!!WAIT TO CHANGE
  # RMSE
  lambda.var.list =c()
  lambda.cov.list =c()
  for (b in (1:length(name_list))){
    temp_var_index= which(RMSE[,b] == min(RMSE[,b]))
    lambda.var.list=cbind(lambda.var.list, lambda.range[temp_var_index])
  }
  if (length(colnames(RMSE.cov))>0){
    for (k in (1:length(colnames(RMSE.cov)))){
      temp_cov_index= which(RMSE.cov[,k] == min(RMSE.cov[,k]))
      lambda.cov.list=cbind(lambda.cov.list, lambda.range[temp_cov_index])
    }
  }
  lambda.var.list <- as.numeric(lambda.var.list)
  if (is.null(lambda.cov.list) == F){
    lambda.cov.list <- as.numeric(lambda.cov.list)
  }
  print(lambda.var.list)
  print(lambda.cov.list)
  final_list = list(lambda.var.list = lambda.var.list, lambda.cov.list = lambda.cov.list, name_list = name_list, length_name = length_name)
  return(final_list)
}
