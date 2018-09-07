EWMA <- function(init_d, lamd, logr){
  sum_return = 0
  sum_lambda = 0
  logr_mean = mean(logr[1:init_d])
  for (i in (1:init_d)){
    temp = (lamd**(init_d-i))*((logr[i] - logr_mean)**2)
    sum_return = sum_return + temp
    sum_lambda = sum_lambda + c(lamd**(i-1))
  }
  sigma = sum_return/sum_lambda
  return(sigma) # Output sigma square
}


# Covar -- Covariance for pairs
Covar <- function(init_d, lamd, logr1, logr2){
  sum_return =0
  sum_lambda =0
  logr1_mean = mean(logr1[1:init_d])
  logr2_mean = mean(logr2[1:init_d])
  for (i in (1:init_d)){
    temp = (lamd**(init_d-i))*((logr1[i] - logr1_mean)*(logr2[i] - logr2_mean))
    sum_return = sum_return + temp
    sum_lambda = sum_lambda + c(lamd**(i-1))
  }
  sigma = sum_return/sum_lambda
  return(sigma)
}