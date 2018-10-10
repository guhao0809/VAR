#设定参数
ed<-as.Date('2018-05-10')
sd<-w.tdaysoffset(-569,ed)$Data[[1]]
version_used<-1
basic.index<-'000906.SH'
process_period<-285
review_period<-285
process_start_day<-286
total_days<-570


#连接mysql
library(DBI)
library(RMySQL)
con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")

#读取指定日期的指数收益率数据，并从中选出有效期内的指数因子
dbSendQuery(con,'SET NAMES gbk')
index_dt_final <- as.data.table(dbGetQuery(con,str_c('select * from factor_return where date between \'', sd, '\' and \'',ed, '\'')))
temp <- as.data.table(dbGetQuery(con,str_c('select f_seccode, f_name from factorlist ')))
index_dt_final<-merge(index_dt_final,temp,by.x = 'f_seccode',by.y = 'f_seccode',all.x = TRUE)
index_dt_final<-index_dt_final[order(f_seccode,date,decreasing = FALSE),]
Industry_Index_dataset <- as.data.table(dbGetQuery(con,"select * from factorlist;"))
Industry_Index_selected<-Industry_Index_dataset[Industry_Index_dataset$version == version_used & sd > as.Date(Industry_Index_dataset$ssd) & ed < as.Date(Industry_Index_dataset$eed),]$f_seccode  
index_dt_final<-index_dt_final[f_seccode %in% Industry_Index_selected]

#生成市场、港股和申万指数集
basic.index = Industry_Index_dataset[f_seccode %in% Industry_Index_selected & f_market == "A" & f_type == "market"]$f_seccode
hs.index =  Industry_Index_dataset[f_seccode %in% Industry_Index_selected & f_market == "H" & f_type == "market"]$f_seccode
sw.industry.indexes = Industry_Index_dataset[f_type == "industry"]$f_seccode

#读取lambda的存储数据，并还原成矩阵的形式
lambda_dataset<-as.data.table(dbGetQuery(con,str_c('select * from all_lambda ')))
cov_factor_list = unique(lambda_dataset$factor_1)
cov_factor_matrix = matrix(0,30,30)
rownames(cov_factor_matrix) = cov_factor_list
colnames(cov_factor_matrix) = cov_factor_list

for (j in (1:length(cov_factor_list))){
  cov_factor_matrix[cov_factor_list[j],cov_factor_list[j]] = lambda_dataset[factor_1 == cov_factor_list[j] & factor_2 == cov_factor_list[j]]$lambda
  if (j > 1){
    for (m in (c(j-1):1)){
      cov_factor_matrix[cov_factor_list[j],cov_factor_list[m]]   = lambda_dataset[factor_1 == cov_factor_list[j] & factor_2 == cov_factor_list[m]]$lambda    
    }
  }
} 

#读取EWMA算法函数
EWMA <- function(lamd, logr){
  # In our model, we assume the mean of retrun is 0.
  #Sustitude loop with vector calculate will save time.
  lamc<-rep(lamd,times=length(ceiling(logr/2)))
  lamc<-cumprod(lamc)
  lamc<-lamc/lamd
  lamc<-lamc[order(lamc)]
  sigma<-sum(lamc*(logr^2))*(1-lamd)
  if (lamd==1) {sigma<-sum(logr^2)/(length(logr)-1)}
  return(sigma) # Output sigma
}
# EWMA(lambda,data_temp$f_return)

# Covar -- Covariance for pairs
Covar <- function(lamd, logr1, logr2){
  #In our model, we assume the mean of retrun is 0.
  #Sustitude loop with vector calculate will save time.
  lamc<-rep(lamd,times=length(logr1))
  lamc<-cumprod(lamc)
  lamc<-lamc/lamd
  lamc<-lamc[order(lamc)]
  cova<-(sum(lamc*(logr1)*(logr2))*(1-lamd))
  if (lamd==1) {cova<-(sum((logr1)*(logr2)))/(length(logr1)-1)}
  return(cova)
  
}

#读取计算生成协方差矩阵的函数
# Index var cov calculation
Index.var.cov.calculation <- function(index_dt_final, process_period, cov_factor_matrix){
  # index_dt_final = selected_index_dt_final
  data = as.data.table(index_dt_final)
  name_list = unique(index_dt_final$f_name)
  A_share_indexes_length = length(c(basic.index,sw.industry.indexes))
  # Name as factor
  data$f_name = as.factor(data$f_name)
  all_indexes_length = length(name_list)
  lambda.var.list = as.numeric(diag(cov_factor_matrix))
  # Needed parameter in calculation
  # data$f_return = 0
  data$RVar_T1 = 0
  cov_df = as.data.frame(matrix(0,nrow=c(process_period+1),ncol = all_indexes_length * c(all_indexes_length+1)/2 -30)) 
  name_list = as.character(unique(data$f_name))
  length_name = length(name_list)
  
  # Calculation
  matrix_index = 0
  # ptm <- proc.time()
  for (j in (1:all_indexes_length)){
    # j = 30
    data_temp = data[f_name == name_list[j]]
    ## Calculate the first day variance in process period
    day1_ewma = EWMA(lambda.var.list[j],data_temp$f_return)
    data_temp$RVar_T1[review_period] = day1_ewma
    
    ## Calculate all covariances in the process period
    # time1 = proc.time()
    if (j > 1){
      for (m in (c(j-1):1)){
        # matrix_index = 0
        matrix_index = matrix_index + 1
        # matrix_index = 29
        # j = 28
        # m = 29
        column_name = paste("cov",j,m,sep ="")
        colnames(cov_df)[matrix_index] = column_name 
        cov_df[column_name][1,] = Covar(cov_factor_matrix[j,m], data_temp$f_return,data[data$f_name == name_list[m]]$f_return)
        number_temp_list = cov_df[,column_name]
        
        for (z in (2:c(total_days-review_period+1))){
          number_temp_list[z] = cov_factor_matrix[j,m]*number_temp_list[c(z-1)]+((1-cov_factor_matrix[j,m])*data_temp$f_return[z]*data[f_name == name_list[m]]$f_return[z])
        }
        cov_df[,column_name] = number_temp_list
        
      }
    }
    # proc.time() - time1
    # Use the recursive formula to calculate variance every day
    for (z in (process_start_day:total_days)){
      temp_var_list = data_temp$RVar_T1
      data_temp$RVar_T1[z] = lambda.var.list[j]*data_temp$RVar_T1[z-1] +(1-lambda.var.list[j])*(data_temp$f_return[z]**2)
    }
    data[f_name == name_list[j]] = data_temp
  }
  
  # proc.time() - ptm
  full_data <- data
  final_data <- data[, .SD[process_start_day:total_days],by = f_name]
  cov.list <- final_data[, .SD[c(total_days-review_period)], by = f_name]$RVar_T1
  # cov_df <- cov_df
  
  left_length = process_period + 1
  # Construct the correlation matrix
  cor.mat = matrix(0,all_indexes_length, all_indexes_length)
  cor.mat[1,1] = 1
  for (i in (2:length_name)){
    cor.mat[i,i] = 1
    for (j in (c(i-1):1)){
      column_name = paste("cov",i,j,sep ="")
      cor.mat[i,j] = cor.mat[j,i] = cov_df[column_name][left_length,]/(sqrt(cov.list[i])*sqrt(cov.list[j]))
    }
  }
  
  cov.mat = t(t(sqrt(cov.list)))%*%sqrt(cov.list)*cor.mat
  colnames(cov.mat) = name_list
  rownames(cov.mat) = name_list
  # variance.list <- final_data[, .SD[c(total_days-review_period)], by = f_name]$RVar_T1
  # final_list = list(full_data = full_data, final_data = final_data, cov_df = cov_df, variance.list = cov.list, cov.mat = cov.mat)
  # final_list = list(cov.mat = cov.mat)
  # return(final_list)
  return(cov.mat)
}

#生成协方差矩阵
all_cov<-Index.var.cov.calculation(index_dt_final, process_period, cov_factor_matrix)

#存储生成的all_cov的矩阵
matrix_cov<-data.table(factor1='S',factor2='S',cov=0,ssd=as.Date('1900-12-30'),eed=as.Date('1900-12-30'))[-1]
for(ii in 1:length(colnames(all_cov))){
  print(ii)
  temp<-data.table(factor1=rep(colnames(all_cov)[ii],times=length(rownames(all_cov))),factor2=rownames(all_cov),cov=all_cov[,ii],ssd=rep(ed,times=length(rownames(all_cov))),eed=rep(ed+30,times=length(rownames(all_cov))))
  matrix_cov<-rbind(matrix_cov,temp)
}
for(ii in 1:length(Industry_Index_dataset$f_name)){
  print(ii)
  matrix_cov[matrix_cov$factor1==Industry_Index_dataset$f_name[ii]]$factor1<-Industry_Index_dataset$f_seccode[ii]
  matrix_cov[matrix_cov$factor2==Industry_Index_dataset$f_name[ii]]$factor2<-Industry_Index_dataset$f_seccode[ii]
}

dbWriteTable(con, "matrix_cov", matrix_cov, overwrite=TRUE, append=FALSE,row.names=F)





