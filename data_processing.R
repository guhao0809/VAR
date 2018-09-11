#连接wind数据库,提取申万行业指数的行情数据
library(RODBC)
library(lubridate)

windriskdb=odbcConnect('wind',uid='lianghua',pwd='lianghua',believeNRows=FALSE)
factorlist<-as.data.table(read.xlsx2('factorlist.xlsx',1,stringsAsFactors = FALSE))
factorlist$ssd<-as.Date(as.numeric(factorlist$ssd),origin = '1899-12-30')
factorlist$eed<-as.Date(as.numeric(factorlist$eed),origin = '1899-12-30')
factorcode<-'\'0\''
for (ii in 1 : length(factorlist$code))
{
  factorcode<-paste(factorcode,',\'',factorlist$code[ii],'\'',sep="")
}

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.ASWSIndexEOD where s_info_windcode in (',factorcode,')',sep='')

factor_return<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(factor_return)<-c('f_seccode','date','preclose','close')
factor_return$date<-ymd(factor_return$date)
factor_return$preclose<-as.numeric(factor_return$preclose)
factor_return$close<-as.numeric(factor_return$close)
factor_return$f_return<-log(factor_return$close/factor_return$preclose)
factor_return<-factor_return[!is.na(factor_return$f_return),]

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.AIndexEODPrices where s_info_windcode in (',factorcode,')',sep='')
A_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(A_factor)<-c('f_seccode','date','preclose','close')
A_factor$date<-ymd(A_factor$date)
A_factor$preclose<-as.numeric(A_factor$preclose)
A_factor$close<-as.numeric(A_factor$close)
A_factor$f_return<-log(A_factor$close/A_factor$preclose)

factor_return<-merge(factor_return,A_factor[,c('date','f_return')],by.x = 'date',by.y = 'date',all.x = TRUE)
factor_return$f_return.x<-factor_return$f_return.x-factor_return$f_return.y

factor_return<-factor_return[!is.na(factor_return$f_return.x),]
factor_return<-as.data.table(factor_return)
factor_return<-factor_return[,c('f_seccode','date','f_return.x')]
names(factor_return)<-c('f_seccode','date','f_return')

factor_return<-rbind(factor_return,A_factor[,c('f_seccode','date','f_return')])

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.HKIndexEODPrices where s_info_windcode in (',factorcode,')',sep='')
H_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(H_factor)<-c('f_seccode','date','preclose','close')
H_factor$date<-ymd(H_factor$date)
H_factor$preclose<-as.numeric(H_factor$preclose)
H_factor$close<-as.numeric(H_factor$close)
H_factor$f_return<-log(H_factor$close/H_factor$preclose)

factor_return<-rbind(factor_return,H_factor[,c('f_seccode','date','f_return')])
factor_return<-factor_return[order(f_seccode,date,decreasing = FALSE),]

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.AIndexWindIndustriesEOD where s_info_windcode in (',factorcode,')',sep='')
w_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(w_factor)<-c('f_seccode','date','preclose','close')
w_factor$date<-ymd(w_factor$date)
w_factor$preclose<-as.numeric(w_factor$preclose)
w_factor$close<-as.numeric(w_factor$close)
w_factor$f_return<-log(w_factor$close/w_factor$preclose)
w_factor<-w_factor[!is.na(w_factor$f_return),]

factor_return<-rbind(factor_return,w_factor[,c('f_seccode','date','f_return')])
factor_return<-factor_return[order(f_seccode,date,decreasing = FALSE),]

factor_return$f_atype<-'S'
factor_return$f_type<-'market'
factor_return[str_detect(f_seccode,'SI')]$f_type<-'industry'
factor_return$version<-1

#连接mysql
library(DBI)
library(RMySQL)
con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
dbWriteTable(con, "factor_return", factor_return, overwrite=TRUE, append=FALSE,row.names=F)

#每日更新数据的逻辑

#连接wind数据库,提取申万行业指数的行情数据
library(RODBC)
library(lubridate)
library(data.table)
library(stringr)
library(xlsx)
library(lubridate)
library(sca)
library(WindR)
w.start()
library(ggplot2)
library(knitr)
library(rmarkdown)

ed<-w.tdaysoffset(-1)$Data[[1]]
ed<-format(ed,format='%Y%m%d')
windriskdb=odbcConnect('wind',uid='lianghua',pwd='lianghua',believeNRows=FALSE)
factorlist<-as.data.table(read.xlsx2('factorlist.xlsx',1,stringsAsFactors = FALSE))
factorlist$ssd<-as.Date(as.numeric(factorlist$ssd),origin = '1899-12-30')
factorlist$eed<-as.Date(as.numeric(factorlist$eed),origin = '1899-12-30')
factorcode<-'\'0\''
for (ii in 1 : length(factorlist$code))
{
  factorcode<-paste(factorcode,',\'',factorlist$code[ii],'\'',sep="")
}

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.ASWSIndexEOD where s_info_windcode in (',factorcode,') and trade_dt=\'',ed,'\'',sep='')

factor_return<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(factor_return)<-c('f_seccode','date','preclose','close')
factor_return$date<-ymd(factor_return$date)
factor_return$preclose<-as.numeric(factor_return$preclose)
factor_return$close<-as.numeric(factor_return$close)
factor_return$f_return<-log(factor_return$close/factor_return$preclose)
factor_return<-factor_return[!is.na(factor_return$f_return),]

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.AIndexEODPrices where s_info_windcode in (',factorcode,') and trade_dt=\'',ed,'\'',sep='')
A_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(A_factor)<-c('f_seccode','date','preclose','close')
A_factor$date<-ymd(A_factor$date)
A_factor$preclose<-as.numeric(A_factor$preclose)
A_factor$close<-as.numeric(A_factor$close)
A_factor$f_return<-log(A_factor$close/A_factor$preclose)

factor_return<-merge(factor_return,A_factor[,c('date','f_return')],by.x = 'date',by.y = 'date',all.x = TRUE)
factor_return$f_return.x<-factor_return$f_return.x-factor_return$f_return.y

factor_return<-factor_return[!is.na(factor_return$f_return.x),]
factor_return<-as.data.table(factor_return)
factor_return<-factor_return[,c('f_seccode','date','f_return.x')]
names(factor_return)<-c('f_seccode','date','f_return')

factor_return<-rbind(factor_return,A_factor[,c('f_seccode','date','f_return')])

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.HKIndexEODPrices where s_info_windcode in (',factorcode,') and trade_dt=\'',ed,'\'',sep='')
H_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(H_factor)<-c('f_seccode','date','preclose','close')
H_factor$date<-ymd(H_factor$date)
H_factor$preclose<-as.numeric(H_factor$preclose)
H_factor$close<-as.numeric(H_factor$close)
H_factor$f_return<-log(H_factor$close/H_factor$preclose)

factor_return<-rbind(factor_return,H_factor[,c('f_seccode','date','f_return')])

sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.AIndexWindIndustriesEOD where s_info_windcode in (',factorcode,') and trade_dt=\'',ed,'\'',sep='')
w_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(w_factor)<-c('f_seccode','date','preclose','close')
w_factor$date<-ymd(w_factor$date)
w_factor$preclose<-as.numeric(w_factor$preclose)
w_factor$close<-as.numeric(w_factor$close)
w_factor$f_return<-log(w_factor$close/w_factor$preclose)
w_factor<-w_factor[!is.na(w_factor$f_return),]

factor_return<-rbind(factor_return,w_factor[,c('f_seccode','date','f_return')])
factor_return<-factor_return[order(f_seccode,date,decreasing = FALSE),]

factor_return$f_atype<-'S'
factor_return$f_type<-'market'
factor_return[str_detect(f_seccode,'SI')]$f_type<-'industry'
factor_return$version<-1

#连接mysql
library(DBI)
library(RMySQL)
con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
dbWriteTable(con, "factor_return", factor_return, overwrite=FALSE, append=TRUE,row.names=F)

#修正数据库中的港股指数的na
factor_return<-dbReadTable(con,'factor_return')
factor_return<-as.data.table(factor_return)
factor_return$date<-as.Date(factor_return$date)
f_hk<-data.table(date=unique(factor_return[f_seccode=='000906.SH']$date))
ssd<-min(factor_return[f_seccode=='LG300.WI']$date)
f_hk<-f_hk[date>=ssd]
f_hk<-merge(f_hk,factor_return[f_seccode=='LG300.WI',c('f_seccode','date','f_return')],by.x = 'date',by.y = 'date',all.x = TRUE)
f_hk$date<-as.Date(f_hk$date)
library(Amelia)
temp<-f_hk[is.na(f_return)]
for(ii in 1:length(temp$date)){
  print(ii)
  n<-which(f_hk==as.character(temp$date[ii]))
  if(n>=570){
   out1<-amelia(f_hk[(n-569):n,c('date','f_return')],m=5)$imputations[[5]]
   temp$f_return[ii]<-out1$f_return[length(out1$f_return)]
  }else{
    out1<-amelia(f_hk[1:570,c('date','f_return')],m=5)$imputations[[5]]
    temp$f_return[ii]<-out1$f_return[n]
  }
}
f_hk[is.na(f_return)]$f_return<-temp[date==f_hk[is.na(f_return)]$date]$f_return
f_hk[is.na(f_seccode)]$f_seccode<-unique(f_hk[!is.na(f_seccode)]$f_seccode)
f_hk$f_atype<-'S'
f_hk$f_type<-'market'
f_hk$version<-1
factor_return<-factor_return[!f_seccode==f_hk$f_seccode[1]]
factor_return<-rbind(factor_return,f_hk)

dbWriteTable(con, "factor_return", factor_return, overwrite=FALSE, append=TRUE,row.names=F)

#生成市场A股的收益率序列
factor_return<-dbReadTable(con,'factor_return')
factor_return$date<-as.Date(factor_return$date)
stock_return<-data.table(date=unique(factor_return$date))
stock_return<-stock_return[order(date)]
sd<-stock_return$date[1]
ed<-stock_return$date[length(stock_return$date)]
sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close, s_dq_tradestatus from newwind.AShareEODPrices where trade_dt between \'',sd,'\' and  \'',ed,'\'',sep='')
windriskdb=odbcConnect('wind',uid='lianghua',pwd='lianghua',believeNRows=FALSE)
temp_return<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
names(temp_return)<-c('seccode','date','preclose','close','trade_status')
temp_return<-as.data.table(temp_return)
temp_return[trade_status=='停牌']$preclose<-NA
temp_return$preclose<-as.numeric(temp_return$preclose)
temp_return$close<-as.numeric(temp_return$close)
temp_return$lnrt<-log(temp_return$close/temp_return$preclose)
temp_return$maxup<-0
temp_return<-as.data.table(temp_return)
temp_return[temp_return$close>=temp_return$preclose*1.1]$maxup<-1

stodata<-data.table(seccode=unique(temp_return$seccode))
stodata$ipodate<-w.wss(stodata$seccode,'ipo_date')$Data[[2]]
stodata$ipodate<-w.asDateTime(stodata$ipodate,asdate = TRUE)
temp_return<-merge(temp_return,stodata,by.x = 'seccode',by.y = 'seccode',all.x = TRUE)

#首先修正A股市场新股刚上市阶段的连续涨停，以EM做法重新填充
temp_return$date<-ymd(temp_return$date)
temp_return<-temp_return[order(seccode,date,decreasing = FALSE),]
temp_list<-temp_return[,.SD[1],by='seccode']
new_stcok<-unique(temp_list[date==ipodate & maxup==1]$seccode)
temp_nomax<-temp_return[maxup==0]
temp_nomax<-temp_nomax[order(seccode,date,decreasing = FALSE),]
temp_nomax<-temp_nomax[,.SD[1],by='seccode']
temp_nomax<-temp_nomax[seccode %in% new_stcok]
for(ii in 1 :length(temp_nomax$seccode)){
  print(ii)
  temp_return[seccode==temp_nomax$seccode[ii] & date<temp_nomax$date[ii]]$lnrt<-NA
}
temp_nalist<-temp_return[is.na(lnrt),c('seccode','date','lnrt')]
temp_full<-temp_nalist[1][-1]
for(ii in 1:length(new_stcok)){
  print(ii)
  if(length(temp_return[seccode==new_stcok[ii]]$date)>=570){
    temp<-temp_return[seccode==new_stcok[ii],c('seccode','date','lnrt')]
    out.a<-amelia(temp[1:570,],idvars = 'seccode',m=5)$imputations[[5]]
  }else{
    temp<-temp_return[seccode==new_stcok[ii],c('seccode','date','lnrt')]
    out.a<-amelia(temp,idvars = 'seccode',m=5)$imputations[[5]]
  }
  temp_full<-rbind(temp_full,out.a)
}

temp_nalist<-merge(temp_nalist,temp_full,by.x = c('seccode','date'),by.y = c('seccode','date'),all.x = TRUE)
temp_nalist<-temp_nalist[,c('seccode','date','lnrt.y')]
names(temp_nalist)<-c('seccode','date','lnrt')
temp_return<-merge(temp_return,temp_nalist,by.x = c('seccode','date'),by.y = c('seccode','date'),all.x = TRUE)
temp_return[!is.na(lnrt.y)]$lnrt.x<-temp_return[!is.na(lnrt.y)]$lnrt.y
temp_return$lnrt.y<-NULL


temp_s_nalist<-temp_nalist[is.na(lnrt)]
for(ii in 1:length(temp_s_nalist$date)){
  print(ii)
  n<-which(f_hk==as.character(temp$date[ii]))
  if(n>=570){
    out1<-amelia(f_hk[(n-569):n,c('date','f_return')],m=5)$imputations[[5]]
    temp$f_return[ii]<-out1$f_return[length(out1$f_return)]
  }else{
    out1<-amelia(f_hk[1:570,c('date','f_return')],m=5)$imputations[[5]]
    temp$f_return[ii]<-out1$f_return[n]
  }
}
f_hk[is.na(f_return)]$f_return<-temp[date==f_hk[is.na(f_return)]$date]$f_return
f_hk[is.na(f_seccode)]$f_seccode<-unique(f_hk[!is.na(f_seccode)]$f_seccode)
f_hk$f_atype<-'S'
f_hk$f_type<-'market'
f_hk$version<-1
factor_return<-factor_return[!f_seccode==f_hk$f_seccode[1]]




#存储lambda的数据
cov<-read.table('cov(1).txt',header = FALSE)
names(cov)<-c('name','lambda')
cov$name<-gsub('return','',cov$name)
sdate<-gsub("净值数据截止日期：","",sdate)
cov$factor_1<-NA
cov$factor_2<-NA
for(ii in 1:length(cov$name)){
  print(ii)
  cov$factor_1[ii]<-substr(cov$name[ii],1,str_locate(cov$name[ii],'I'))
  cov$factor_2[ii]<-substr(cov$name[ii],(str_locate(cov$name[ii],'I')+1),nchar(cov$name[1]))
}
cov<-cov[,c('factor_1','factor_2','lambda')]
cov$ssd<-as.Date('2018-09-06')
cov$eed<-as.Date('2018-10-07')

var<-read.table('var(1).txt',header = FALSE)
names(var)<-c('factor_1','lambda')
var$factor_2<-var$factor_1
var<-var[,c('factor_1','factor_2','lambda')]
var$ssd<-as.Date('2018-09-06')
var$eed<-as.Date('2018-10-07')


all_lambda<-rbind(cov,var)
dbWriteTable(con, "all_lambda", all_lambda, overwrite=TRUE, append=FALSE,row.names=F)


#暂时不启用,待恒生港股通指数满足570天了再进行处理。
#f_hk1<-data.table(date=unique(factor_return[f_seccode=='000906.SH']$date))
#ssd1<-min(factor_return[f_seccode=='HSHKI.HI']$date)
#f_hk1<-f_hk1[date>=ssd1]
#f_hk1<-merge(f_hk1,factor_return[f_seccode=='HSHKI.HI',c('f_seccode','date','f_return')],by.x = 'date',by.y = 'date',all.x = TRUE)
#f_hk1$date<-as.Date(f_hk1$date)
#library(Amelia)
#temp<-f_hk1[is.na(f_return)]
#for(ii in 1:length(temp$date)){
#   print(ii)
#   n<-which(f_hk1==as.character(temp$date[ii]))
#   if(n>=570){
#     out1<-amelia(f_hk1[(n-569):n,c('date','f_return')],m=5)$imputations[[5]]
#     temp$f_return[ii]<-out1$f_return[length(out1$f_return)]
#   }else{
#     out1<-amelia(f_hk1[1:570,c('date','f_return')],m=5)$imputations[[5]]
#     temp$f_return[ii]<-out1$f_return[n]
#   }
# }
# f_hk1[is.na(f_return)]$f_return<-temp[date==f_hk1[is.na(f_return)]$date]$f_return
# f_hk1[is.na(f_seccode)]$f_seccode<-unique(f_hk1[!is.na(f_seccode)]$f_seccode)
# f_hk1$f_atype<-'S'
# f_hk1$f_type<-'market'
# f_hk1$version<-1
# factor_return<-factor_return[!f_seccode==f_hk1$f_seccode[1]]
# factor_return<-rbind(factor_return,f_hk1)
# 
# write.table(factor_return,'factor_return.txt',row.names = FALSE)



