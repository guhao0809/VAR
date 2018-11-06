td<-as.Date(w.tdays('2018-03-15','2018-10-31')$Data[[1]])
for (ii in 1:length(td)){
  print(ii)
  ed<-td[ii]
  sd<-w.tdaysoffset(-569,ed)$Data[[1]]
  lsd<-w.tdaysoffset(-284,ed)$Data[[1]]
  version_used<-1
  process_period<-285
  review_period<-285
  process_start_day<-286
  total_days<-570
  basic.index<-'000906.SH'
  span<-285
  lambda<-matrix_lambda(sd,ed,process_period)
  library(DBI)
  library(RMySQL)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  dbWriteTable(con, "lambda", lambda, overwrite=FALSE, append=TRUE,row.names=F)
  matrix_cov<-cov_store(ed,sd,version_used,basic.index,review_period,process_start_day,total_days)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  dbWriteTable(con, "factor_cov", matrix_cov, overwrite=FALSE, append=TRUE,row.names=F)
  matrix_cof<-cof_stor(lsd,ed,process_period)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  dbWriteTable(con, "factor_exposure", matrix_cof, overwrite=FALSE, append=TRUE,row.names=F)
  cons<-dbListConnections(MySQL())
  for(con in cons) 
  {
    dbDisconnect(con)
  }
  gc()
}


data_basic<-function(adr,dbadr){
  #每周一采取定期回顾的方式，以temp的形式对历史的存储数据进行更新
  #每周的正常工作日，则每日进行数据库数据的更新与维护
  
  
  library(data.table)
  library(stringr)
  library(xlsx)
  library(WindR)
  w.start()
  library(lubridate)
  library(DBI)
  library(RMySQL)
  
  
  adr='Y:\\Risk Control\\rawdata\\'
  dbadr<-'//172.16.23.200/共享/riskplantform/db/'
  #td 函数生成日期序列数据
  
  #读取基金列表信息，定期更新组合的列表信息
  fcode_dc<-as.data.table(read.xlsx2(paste(dbadr,'portfoliobasic_dc.xlsx',sep=''),1,stringsAsFactors = FALSE)[c('dccode','组合简称','组合成立日','shareopdate','数据中心代码','组合代码','账户组','内部分类','lvl','策略组')])
  names(fcode_dc)<-c('code','name','setupdate','sopdate','zcode','mcode','issuetype','innerclass','lvl','dept')
  fcode_dc$setupdate<-as.Date(as.numeric(fcode_dc$setupdate),origin='1899-12-30')
  fcode_dc$sopdate<-as.Date(as.numeric(fcode_dc$sopdate),origin='1899-12-30')
  save(fcode_dc,file=paste(adr,'fcode_dc.Rdata',sep=""))
  
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  dbSendQuery(con,'SET NAMES gbk')
  
  dbWriteTable(con, "fundbasic", fcode_dc, overwrite=FALSE, append=TRUE,row.names=F)
  
  #读取组合净资产的信息，更新组合的资产数据
  fasset<-data.table(attach(paste(adr,'fasset_dc.Rdata',sep=''),pos=2)$fasset)
  detach(pos=2)
  eed<- max(fasset$date)
  std<- min(fasset$date)
  
  wd<-data.table(date=as.Date(w.tdays(as.Date(w.tdaysoffset(-1,std)$Data[,1]),eed,Days='Alldays')$Data[,1]))
  td<-data.table(date=as.Date(w.tdays(as.Date(w.tdaysoffset(-1,std)$Data[,1]),eed,Days='Trading')$Data[,1]))
  ztd<-data.table(date=as.Date(w.tdays(as.Date(w.tdaysoffset(-1,std)$Data[,1]),eed,Days='Trading')$Data[,1]))
  
  yl<-unique(year(td$date))
  q1l<-as.Date(str_c(yl,'03','31',sep='-'))
  q2l<-as.Date(str_c(yl,'06','30',sep='-'))
  q3l<-as.Date(str_c(yl,'09','30',sep='-'))
  q4l<-as.Date(str_c(yl,'12','31',sep='-'))
  ql<-data.table(date=c(q1l,q2l,q3l,q4l))  
  
  wd$pred<-wd$date-1
  
  pretd<-c(as.Date(w.tdaysoffset(-1,td$date[1])$Data[,1]),td$date[-length(td$date)])
  td$pretd<-pretd
  
  
  ztd<-rbind(ztd,ql[ql$date>=min(td$date)& ql$date<=max(td$date)])
  ztd<-unique(ztd)
  ztd<-ztd[order(date)]
  
  preztd<-c(as.Date(w.tdaysoffset(-1,ztd$date[1])$Data[,1]),ztd$date[-length(ztd$date)])
  ztd$preztd<-preztd
  
  
  zotd<-merge(wd,td,by='date',all.x=TRUE)
  zotd<-merge(zotd,ztd,by='date',all.x=TRUE)
  
  
  dbWriteTable(con, "zotd", zotd, overwrite=FALSE, append=TRUE,row.names=F)
  
  
  #净资产数据的整理
  fasset<-data.table(attach(paste(adr,'fasset_dc.Rdata',sep=''),pos=2)$fasset)
  detach(pos=2)
  fasset<-fasset[order(code,date,na.last = TRUE,decreasing = FALSE),]
  fasset<-fasset[unv!=0]
  fasset[is.na(inv)]$inv<-0
  fasset[is.na(outv)]$outv<-0
  fasset[is.na(rt_dc)]$rt_dc<-0
  fasset[is.na(div)]$div<-0
  fasset[is.na(split)]$split<-1
  fasset$snav<-as.numeric(NA)
  fasset$preunv<-as.numeric(NA)
  fasset$preunv_td<-as.numeric(NA)
  fasset$preunv_ztd<-as.numeric(NA)
  
  temp_fa<-data.table(dbGetQuery(con,str_c("select * from rm.fundasset where date in ('",unique(c(zotd[date==std]$pred,zotd[date==std]$pretd)),"')")))
  temp_fa$date<-as.Date(temp_fa$date)
  temp_fa<-rbind(fasset,temp_fa)
  
  fasset<-merge(fasset,zotd,by='date',all.x=TRUE)
  fasset<-merge(fasset,temp_fa[,.(code,date,preu=unv)],by.x=c('code','pred'), by.y=c('code','date'), all.x=TRUE)
  fasset<-merge(fasset,temp_fa[,.(code,date,preu_td=unv)],by.x=c('code','pretd'),by.y=c('code','date'), all.x=TRUE)
  fasset<-merge(fasset,temp_fa[,.(code,date,preu_ztd=unv)],by.x=c('code','preztd'),by.y=c('code','date'), all.x=TRUE)
  fasset<-merge(fasset,temp_fa[,.(code,date,prenav=nav)],by.x=c('code','pred'),by.y=c('code','date'), all.x=TRUE)
  fasset$preunv<-fasset$preu
  fasset$preunv_td<-fasset$preu_td
  fasset$preunv_ztd<-fasset$preu_ztd
  fasset$snav<-fasset$prenav
  
  
  #成立首日的基金，其前一日单位净值为1，snav为其inv
  if (length(fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv)!=0)
  {
    fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv<-1
    fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv_td<-1
    fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv_ztd<-1
    fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$snav<-fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$inv
  }
  
  #成立首日的A份额，其前一日单位净值为母基金前一日单位净值，snav为其份额
  #成立首日的非A份额，其前一日单位净值为A份额前一日单位净值，snav为其份额
  temp<-merge(fasset,fcode_dc[,.(code,sopdate)],by.x='code',by.y='code',all.x=TRUE)
  temp<-temp[order(code,date,na.last = TRUE,decreasing = FALSE),]
  tt<-temp[date==sopdate]
  for (ii in 1:length(tt$code))
  {
    if(str_detect(tt$name[ii],'A'))
    {      
      fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii]) ]$preunv_td<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'A','')]$preunv_td      
      fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii]) ]$preunv<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'A','')]$preunv
      fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii]) ]$preunv_ztd<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'A','')]$preunv_ztd
    } 
    
    if(str_detect(tt$name[ii],'[BCDE]') )
    {      
      fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii])]$preunv_td<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'[BCDE]','A')]$preunv_td      
      fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii])]$preunv<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'[BCDE]','A')]$preunv
      fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii])]$preunv_ztd<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'[BCDE]','A')]$preunv_ztd   
    } 
  }  
  fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$sopdate)]$snav<-fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$sopdate)]$inv
  #筛选一下，去掉早于份额起始日的数据
  tpcd<-temp[date>=sopdate]
  fasset<-fasset[str_c(code,date) %in% str_c(tpcd$code,tpcd$date)]
  fasset[is.na(preunv) & !(pred %in% td$date) & date %in% td$date]$preunv<-fasset[is.na(preunv) & !(pred %in% td$date) & date %in% td$date]$preunv_td
  
  dbWriteTable(con, "fundasset", fasset[,.(code,date,name,innercode,nav,share,unv,accunv,preunv_dc,tav,treturn,rt_dc,inv,outv,div,split,snav,preunv,preunv_td,preunv_ztd)], overwrite=FALSE, append=TRUE,row.names=F)
  
  #对fundasset写入的历史数据根据最新数据进行更新
  temp_asset <- as.data.table(dbGetQuery(con,str_c('select * from fundasset where date between \'', std, '\' and \'',eed, '\'')))
  fasset_g<-fasset[,.(code,date,name,innercode,nav,share,unv,accunv,preunv_dc,tav,treturn,rt_dc,inv,outv,div,split,snav,preunv,preunv_td,preunv_ztd)]
  fasset_g$pcode<-str_c(fasset_g$code,fasset_g$date,fasset_g$name,fasset_g$innercode,fasset_g$nav,fasset_g$share,fasset_g$unv,fasset_g$accunv,fasset_g$preunv_dc,fasset_g$tav,fasset_g$treturn,fasset_g$rt_dc,fasset_g$inv,fasset_g$outv,fasset_g$div,fasset_g$split,fasset_g$snav,fasset_g$preunv,fasset_g$preunv_td,fasset_g$preunv_ztd)
  temp_asset$pcode<-str_c(temp_asset$code,temp_asset$date,temp_asset$name,temp_asset$innercode,temp_asset$nav,temp_asset$share,temp_asset$unv,temp_asset$accunv,temp_asset$preunv_dc,temp_asset$tav,temp_asset$treturn,temp_asset$rt_dc,temp_asset$inv,temp_asset$outv,temp_asset$div,temp_asset$split,temp_asset$snav,temp_asset$preunv,temp_asset$preunv_td,temp_asset$preunv_ztd)
  update_asset<-fasset_g[fasset_g$pcode!=temp_asset$pcode]
  update_asset$pcode<-NULL
  if(length(update_asset$code)!=0){
    dbWriteTable(con, "fundasset_temp", update_asset, overwrite=TRUE, append=FALSE,row.names=F)
    dbSendQuery(con,"update fundasset,fundasset_temp set fundasset.nav=fundasset_temp.nav,fundasset.share=fundasset_temp.share,fundasset.unv=fundasset_temp.unv,fundasset.accunv=fundasset_temp.accunv,fundasset.preunv_dc=fundasset_temp.preunv_dc,fundasset.tav=fundasset_temp.tav,fundasset.treturn=fundasset_temp.treturn,fundasset.rt_dc=fundasset_temp.rt_dc,fundasset.inv=fundasset_temp.inv,fundasset.outv=fundasset_temp.outv,fundasset.div=fundasset_temp.div,fundasset.split=fundasset_temp.split,fundasset.snav=fundasset_temp.snav,fundasset.preunv=fundasset_temp.preunv,fundasset.preunv_td=fundasset_temp.preunv_td,fundasset.preunv_ztd=fundasset_temp.preunv_ztd where fundasset.code=fundasset_temp.code and fundasset.date=fundasset_temp.date")
  }
  
  #持仓数据的整理
  fhold<-data.table(attach(paste(adr,'fhold_dc.Rdata',sep=''),pos=2)$fhold)
  detach(pos=2)
  fhold<-fhold[pos != '',.(secname=secname[1],volume=sum(volume),ctvalue=sum(ctvalue),cvalue=sum(cvalue),taccvalue=sum(taccvalue),fvalue=sum(fvalue),ctprice=sum(ctvalue)/sum(volume),cprice=sum(cvalue)/sum(volume),fprice=sum(fprice)/sum(volume),tacc=sum(taccvalue)/sum(volume)),by=.(code,name,date,seccode,market,pos,atype,tradeable)]
  fhold<-fhold[order(code,seccode,date,na.last = TRUE,decreasing = FALSE),]
  fhold<-merge(fhold,fasset[,.(code,date,nav)],by=c('code','date'),all.x=TRUE)
  fhold$cratio<-fhold$cvalue/fhold$nav
  fhold<-fhold[,.(code,date,name,seccode,secname,market,volume,ctvalue,cvalue,taccvalue,pos,atype,tradeable,fvalue,ctprice,cprice,fprice,tacc,nav,cratio)]
  dbWriteTable(con, "fundhold", fhold, overwrite=FALSE, append=TRUE,row.names=F)
  
  #对fundhold的历史数据进行更新
  temp_hold <- as.data.table(dbGetQuery(con,str_c('select * from fundhold where date between \'', std, '\' and \'',eed, '\'')))
  fhold_g<-fhold
  fhold_g$pcode<-str_c(fhold_g$code,fhold_g$date,fhold_g$name,fhold_g$seccode,fhold_g$secname,fhold_g$market,fhold_g$volume,fhold_g$ctvalue,fhold_g$cvalue,fhold_g$taccvalue,fhold_g$pos,fhold_g$atype,fhold_g$tradeable,fhold_g$fvalue,fhold_g$ctprice,fhold_g$cprice,fhold_g$fprice,fhold_g$tacc,fhold_g$nav,fhold_g$cratio)
  temp_hold$pcode<-str_c(temp_hold$code,temp_hold$date,temp_hold$name,temp_hold$seccode,temp_hold$secname,temp_hold$market,temp_hold$volume,temp_hold$ctvalue,temp_hold$cvalue,temp_hold$taccvalue,temp_hold$pos,temp_hold$atype,temp_hold$tradeable,temp_hold$fvalue,temp_hold$ctprice,temp_hold$cprice,temp_hold$fprice,temp_hold$tacc,temp_hold$nav,temp_hold$cratio)
  update_hold<-fhold_g[fhold_g$pcode!=temp_hold$pcode]
  update_hold$pcode<-NULL
  if(length(update_hold$code)!=0){
    dbWriteTable(con, "fundhold_temp", update_hold, overwrite=TRUE, append=FALSE,row.names=F)
    dbSendQuery(con,"update fundhold,fundhold_temp set fundhold.seccode=fundhold_temp.seccode,fundhold.secname=fundhold_temp.secname,fundhold.market=fundhold_temp.market,fundhold.volume=fundhold_temp.volume,fundhold.ctvalue=fundhold_temp.ctvalue,fundhold.cvalue=fundhold_temp.cvalue,fundhold.taccvalue=fundhold_temp.taccvalue,fundhold.pos=fundhold_temp.pos,fundhold.atype=fundhold_temp.atype,fundhold.tradeable=fundhold_temp.tradeable,fundhold.fvalue=fundhold_temp.fvalue,fundhold.ctprice=fundhold_temp.ctprice,fundhold.cprice=fundhold_temp.cprice,fundhold.fprice=fundhold_temp.fprice,fundhold.tacc=fundhold_temp.tacc,fundhold.nav=fundhold_temp.nav, fundhold.cratio=fundhold_temp.cratio where fundhold.code=fundhold_temp.code and fundhold.date=fundhold_temp.date")
  }
  
  
  #交易数据的整理
  #根据运营的说法，买入和卖出证券的情况下，他们的凭证应该是：
  #1买入的情况下  清算金额=成本金额+利息总额+交易费用 其中成本金额=净价金额-扣掉的利息税 
  #2卖出的情况下  清算金额=成本金额+利息总额-交易费用 其中成本金额=净价金额+返还的利息税
  #3兑付和付息的情况 与卖出等同
  #按照数据中心的字段，对于买入类型的而言，应该有fvalue=cvalue+taccvalue+fee+tax，但数据中心数据较乱，cvalue有些是净价市值，有些加了利息税，有些加了费用，导致字段的值很不稳定，
  #而且有时候fvalue 和cvalue是反过来的，因此采用下面的策略进行清洗：
  #1先判断cvalue和fvalue的大小，将fvalue全部清洗为清算金额
  #2根据清算金额，结合买卖方向倒算cvalue
  #3根据cvalue计算fvalue
  
  ftrade<-attach(paste(adr,'ftrade_dc.Rdata',sep=''),pos=2)$ftrade
  detach(pos=2)
  ftrade<-as.data.table(ftrade)
  ftrade<-ftrade[order(code,date,na.last = TRUE,decreasing = FALSE),]
  
  ftrade[is.na(tax)]$tax<-0
  ftrade[is.na(taccvalue)]$taccvalue<-0
  ftrade[is.na(fee)]$fee<-0
  ftrade[is.na(tacc)]$tacc<-0
  
  
  ftrade$clvalue<-ftrade$fvalue
  ftrade[cvalue >0 & abs(cvalue) > abs(clvalue)]$clvalue<-ftrade[cvalue >0 & abs(cvalue) > abs(clvalue)]$cvalue
  #卖出的情况比较复杂，可以分为：
  #1、卖出，有应计利息的情况，这种情况下，一般应计利息>费用，因此清算金额>成本金额，找出清算金额<成本金额的，进行修正
  ftrade[cvalue <0 & taccvalue!=0 & abs(cvalue) > abs(clvalue)]$clvalue<-ftrade[cvalue <0 & taccvalue!=0 & abs(cvalue) > abs(clvalue)]$cvalue
  #2、卖出，无应计利息的情况，这种情况下，清算金额<成本金额，找出清算金额>成本金额的，进行修正
  ftrade[cvalue <0 & taccvalue==0 & abs(cvalue) < abs(clvalue)]$clvalue<-ftrade[cvalue <0 & taccvalue==0 & abs(cvalue) < abs(clvalue)]$cvalue
  
  #由清算金额得到净价金额
  ftrade[clvalue>0]$cvalue<-ftrade[clvalue>0]$clvalue-abs(ftrade[clvalue>0]$taccvalue)-abs(ftrade[clvalue>0]$tax)-abs(ftrade[clvalue>0]$fee)
  ftrade[clvalue<0]$cvalue<--(abs(ftrade[clvalue<0]$clvalue)-abs(ftrade[clvalue<0]$taccvalue)-abs(ftrade[clvalue<0]$tax)+abs(ftrade[clvalue<0]$fee))
  
  #由净价金额得到全价金额
  ftrade[clvalue>0]$fvalue<-ftrade[clvalue>0]$cvalue+ftrade[clvalue>0]$taccvalue
  ftrade[clvalue<0]$fvalue<-ftrade[clvalue<0]$cvalue+ftrade[clvalue<0]$taccvalue
  
  ftrade$cprice<-ftrade$cvalue/ftrade$volume
  ftrade$fprice<-ftrade$fvalue/ftrade$volume
  ftrade$clprice<-ftrade$clvalue/ftrade$volume
  
  fundtrade<-ftrade[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),clvalue=sum(clvalue),taccvalue=sum(taccvalue),fee=sum(fee),tax=sum(tax),tacc=sum(taccvalue)/sum(volume),fprice=sum(fvalue)/sum(volume),cprice=sum(cvalue)/sum(volume),clprice=sum(clvalue)/sum(volume)),by=.(code,date,seccode,market,atype,pos,ttype,tattr)]
  fundtrade<-fundtrade[,.(code,date,seccode,market,atype,volume,fprice,cprice,clprice,cvalue,fvalue,clvalue,taccvalue,fee,ttype,tattr,pos,tax,tacc)]
  library(DBI)
  library(RMySQL)
  dbWriteTable(con, "fundtrade", fundtrade, overwrite=FALSE, append=TRUE,row.names=F)
  
  #对fundtrade的历史数据进行更新
  temp_trade <- as.data.table(dbGetQuery(con,str_c('select * from fundtrade where date between \'', std, '\' and \'',eed, '\'')))
  ftrade_g<-fundtrade
  ftrade_g$pcode<-str_c(ftrade_g$code,ftrade_g$name,ftrade_g$date,ftrade_g$seccode,ftrade_g$market,ftrade_g$atype,ftrade_g$volume,ftrade_g$fprice,ftrade_g$cprice,ftrade_g$clprice,ftrade_g$cvalue,ftrade_g$fvalue,ftrade_g$clvalue,ftrade_g$taccvalue,ftrade_g$fee,ftrade_g$ttype,ftrade_g$tattr,ftrade_g$pos,ftrade_g$tax,ftrade_g$tacc)
  temp_trade$pcode<-str_c(temp_trade$code,temp_trade$name,temp_trade$date,temp_trade$seccode,temp_trade$market,temp_trade$atype,temp_trade$volume,temp_trade$fprice,temp_trade$cprice,temp_trade$clprice,temp_trade$cvalue,temp_trade$fvalue,temp_trade$clvalue,temp_trade$taccvalue,temp_trade$fee,temp_trade$ttype,temp_trade$tattr,temp_trade$pos,temp_trade$tax,temp_trade$tacc)
  update_trade<-ftrade_g[ftrade_g$pcode!=temp_trade$pcode]
  update_trade$pcode<-NULL
  if(length(update_trade$code)!=0){
    dbWriteTable(con, "fundtrade_temp", update_hold, overwrite=TRUE, append=FALSE,row.names=F)
    dbSendQuery(con,"update fundtrade,fundtrade_temp set fundtrade.seccode=fundtrade_temp.seccode,fundtrade.market=fundtrade_temp.market,fundtrade.atype=fundtrade_temp.atype,fundtrade.volume=fundtrade_temp.volume,fundtrade.fprice=fundtrade_temp.fprice,fundtrade.cprice=fundtrade_temp.cprice,fundtrade.clprice=fundtrade_temp.clprice,fundtrade.cvalue=fundtrade_temp.cvalue,fundtrade.fvalue=fundtrade_temp.fvalue,fundtrade.clvalue=fundtrade_temp.clvalue,fundtrade.taccvalue=fundtrade_temp.taccvalue,fundtrade.fee=fundtrade_temp.fee,fundtrade.ttype=fundtrade_temp.ttype,fundtrade.tattr=fundtrade_temp.tattr,fundtrade.pos=fundtrade_temp.pos,fundtrade.tax=fundtrade_temp.tax, fundtrade.tacc=fundtrade_temp.tacc where fundtrade.code=fundtrade_temp.code and fundtrade.date=fundtrade_temp.date")
  }
  
  
  ftrade<-ftrade[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),taccvalue=sum(taccvalue),fee=sum(fee),tax=sum(tax),tacc=sum(tacc)),by=.(code,date,seccode,market,atype,pos)]
  
  
  #取出持仓、交易、日期、净资产数据，计算得到组合个券日收益率表
  
  #根据持仓和交易数据，合成全持仓数据
  
  hnt_temp<-unique(rbind(fhold[,.(code,date,seccode,market,atype)],ftrade[,.(code,date,seccode,market,atype)]))
  hnt_temp<-merge(hnt_temp,zotd,by=c('date'),allow.cartesian = TRUE)
  hnt_temp<-merge(hnt_temp,fhold[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),ctvalue=sum(ctvalue)),by=.(code,date,seccode,pos)],by=c('code','date','seccode'),all.x=TRUE)
  hnt_temp<-merge(hnt_temp,fhold[,.(pvolume=sum(volume),pcvalue=sum(cvalue),pfvalue=sum(fvalue),pctvalue=sum(ctvalue)),by=.(code,date,seccode,pos)],by.x=c('code','pred','seccode','pos'),by.y=c('code','date','seccode','pos'),all.x=TRUE)
  ftrade_cl<-merge(ftrade[cvalue>0,.(bvolume=sum(volume),bcvalue=sum(cvalue),bfvalue=sum(fvalue)),by=.(code,date,seccode,pos)],ftrade[cvalue<0,.(svolume=sum(volume),scvalue=sum(cvalue),sfvalue=sum(fvalue)),by=.(code,date,seccode,pos)],by=c('code','date','seccode','pos'),all=TRUE)
  hnt_temp<-merge(hnt_temp,ftrade_cl,by=c('code','date','seccode','pos'),all.x=TRUE)
  
  #计算个券收益率数据,c代表净价，f全价，h为买入日初卖出日终的方法
  hnt_temp[is.na(cvalue)]$cvalue<-0
  hnt_temp[is.na(scvalue)]$scvalue<-0
  hnt_temp[is.na(pcvalue)]$pcvalue<-0
  hnt_temp[is.na(bcvalue)]$bcvalue<-0
  hnt_temp[is.na(fvalue)]$fvalue<-0
  hnt_temp[is.na(sfvalue)]$sfvalue<-0
  hnt_temp[is.na(pfvalue)]$pfvalue<-0
  hnt_temp[is.na(bfvalue)]$bfvalue<-0
  
  
  hnt_temp$secr_c_h<-(hnt_temp$cvalue-hnt_temp$scvalue)/(hnt_temp$pcvalue+hnt_temp$bcvalue)-1
  hnt_temp$secr_f_h<-(hnt_temp$fvalue-hnt_temp$sfvalue)/(hnt_temp$pfvalue+hnt_temp$bfvalue)-1
  
  dbWriteTable(con, "hnt", hnt_temp, overwrite=FALSE, append=TRUE,row.names=F)
  
  #业绩分解数据处理
  fdec<-data.table(attach(paste(adr,'fdec_dc.Rdata',sep=''),pos=2)$fdec)
  detach(pos=2)
  fdec<-fdec[,.(gain=sum(gain),rctr=sum(rctr)),by=.(code,date,seccode,market,atype,type)]
  dbWriteTable(con, "fdec", fdec, overwrite=FALSE, append=TRUE,row.names=F)
  
  #存储市场指数数据
  mindex<-data.table(attach(paste(adr,'indexprice.Rdata',sep=''),pos=2)$indexprice)
  detach(pos=2)
  names(mindex)[1]<-'indcode_dc'
  pbench<-unique(data.table(read.xlsx2(paste(dbadr,'portbench.xlsx',sep=''),1,stringsAsFactors = FALSE))[,.(indcode,indcode_dc)])
  mindex<-merge(mindex,pbench[,.(indcode,indcode_dc)],by='indcode_dc',all.x=TRUE)
  
  pmi<-data.table(dbGetQuery(con, str_c('SELECT indcode,date,close FROM rm.mindex where date between \'', as.Date(w.tdaysoffset(-1,std)$Data[,1]), '\' and \'', std-1, '\'')))
  pmi$date<-as.Date(pmi$date)
  
  temp<-rbind(mindex[,.(indcode,date,close)],pmi[date<min(mindex$date)])
  temp<-temp[order(indcode,date)]
  #得到preclose和涨跌幅的数字
  mi_aft<-temp[-1,]
  mi_pre<-temp[-length(temp$date),]
  mi_aft$preclose<-mi_pre$close
  mi<-mi_aft[indcode==mi_pre$indcode]
  mi$rt<-mi$close/mi$pre-1
  
  #对于定存类的基准，获取其收益率值
  indcode_cash<-c('DEPO1Y.WI','DEPO3Y.WI','DEMAND.WI','CALLDEPO7D.WI')
  icode<-c('M0009808','M0009810','M0009805','M0041348')
  
  library(WindR)
  w.start()
  cash_int<-as.data.table(w.edb(icode,'2000-01-01',eed,'Fill=Previous')$Data)
  cash_int$DATETIME<-as.Date(cash_int$DATETIME)
  sde<-max(cash_int[DATETIME<=std]$DATETIME)
  cash_int<-cash_int[DATETIME>=sde]
  cash_int$ed<-c(cash_int$DATETIME[-1],eed+1)
  
  ci<-data.table(indcode='s',date=as.Date('1900-01-01'),close=1,preclose=1,rt=1)[-1,]
  
  
  for (ii in 1 : length(indcode_cash))
  {
    ci<-rbind(ci,data.table(indcode=rep(indcode_cash[ii],times=length(wd$date)),date=wd$date,close=rep(0,times=length(wd$date)),preclose=rep(0,times=length(wd$date)),rt=rep(0,times=length(wd$date))))
    
  }
  
  
  
  for (ii in 1 : length(cash_int$DATETIME))
  {
    for (jj in 1 : length(icode))
    {
      ci[date>=cash_int$DATETIME[ii] & date<cash_int$ed[ii] & indcode==indcode_cash[jj]]$close<-cash_int[ii,which(names(cash_int)==icode[jj]),with=FALSE]/100
    }
  }
  
  ci365<-ci
  ci365$indcode<-str_c(ci365$indcode,'365')
  
  temp<-ci$date
  month(temp)<-12
  day(temp)<-31
  ci$rt<-ci$close/yday(temp)
  ci365$rt<-ci$close/365
  ci<-rbind(ci,ci365)
  bench_index<-rbind(mi,ci)
  #合并基准利率类和市场指数类，作为基准计算的基础数据，并保存
  dbWriteTable(con, "mindex",bench_index, overwrite=FALSE, append=TRUE,row.names=F)
  
  #生成收益率的数据
  tmp_rt<-fasset[,.(code,date,rt=unv/preunv-1,type='ad')]
  tmp_rt_td<-fasset[,.(code,date,rt=unv/preunv_td-1,type='td')]
  tmp_rt_ztd<-fasset[,.(code,date,rt=unv/preunv_ztd-1,type='ztd')]
  tmp_rt_nav<-fasset[,.(code,date,rt=nav/snav-1,type='nav')]
  
  #对于一些特殊的基金，直接给定特定日期的收益率
  tmp_rt[code=='F166010' & date == as.Date('2014-06-17')]$rt<--0.003214470
  tmp_rt[code=='155039' & date == as.Date('2014-06-17')]$rt<-0
  tmp_rt[code=='155040' & date == as.Date('2014-06-17')]$rt<--0.01
  
  tmp_rt[code=='166016' & date == as.Date('2013-07-30')]$rt<-0.000235121413995509
  tmp_rt[code=='166016' & date == as.Date('2013-07-31')]$rt<--7.65857575039108E-05
  tmp_rt[code=='166016' & date == as.Date('2014-01-30')]$rt<-0.00055181706813956
  tmp_rt[code=='166016' & date == as.Date('2014-02-07')]$rt<-0.00248802866360442
  tmp_rt[code=='166016' & date == as.Date('2014-07-30')]$rt<--0.000331599246851444
  tmp_rt[code=='166016' & date == as.Date('2014-07-31')]$rt<-0.000162615555370449
  tmp_rt[code=='166016' & date == as.Date('2015-01-30')]$rt<-0.000375049857613607
  tmp_rt[code=='166016' & date == as.Date('2015-02-02')]$rt<-0.000944351822492528
  tmp_rt[code=='166016' & date == as.Date('2015-07-30')]$rt<-0.000444195589399898
  tmp_rt[code=='166016' & date == as.Date('2015-07-31')]$rt<-0.000554179764699958
  tmp_rt[code=='166016' & date == as.Date('2016-01-29')]$rt<-0.00045106745531065
  tmp_rt[code=='166016' & date == as.Date('2016-02-01')]$rt<--0.000145651883250331
  tmp_rt[code=='166016' & date == as.Date('2016-02-02')]$rt<-0.00564923404226114
  tmp_rt[code=='F166021' & date %in% as.Date(c('2014-5-27','2014-5-28','2014-5-29','2014-11-27','2014-11-28','2014-12-1','2015-5-27','2015-5-28','2015-5-29','2015-11-27','2015-11-30','2015-12-1','2016-5-27','2016-5-30','2016-12-1'))]$rt<-c(0.000526973760285099,0.000554261432409442,0.000763098390563322,0.000280849325514865,-1.8637270154942E-06,7.67581495579872E-05,-0.000256862288835813,-0.000100101673465858,-0.000434187295041077,0.000708130000000029,0.000611790000000001,0.00094751000000004,0.000303401600000086,0.000337068957248166,-0.00100200400801598)
  
  tmp_rt_td[code=='F166010' & date == as.Date('2014-06-17')]$rt<--0.003214470
  tmp_rt_td[code=='155039' & date == as.Date('2014-06-17')]$rt<-0
  tmp_rt_td[code=='155040' & date == as.Date('2014-06-17')]$rt<--0.01
  
  tmp_rt_td[code=='166016' & date == as.Date('2013-07-30')]$rt<-0.000235121413995509
  tmp_rt_td[code=='166016' & date == as.Date('2013-07-31')]$rt<--7.65857575039108E-05
  tmp_rt_td[code=='166016' & date == as.Date('2014-01-30')]$rt<-0.00055181706813956
  tmp_rt_td[code=='166016' & date == as.Date('2014-02-07')]$rt<-0.00248802866360442
  tmp_rt_td[code=='166016' & date == as.Date('2014-07-30')]$rt<--0.000331599246851444
  tmp_rt_td[code=='166016' & date == as.Date('2014-07-31')]$rt<-0.000162615555370449
  tmp_rt_td[code=='166016' & date == as.Date('2015-01-30')]$rt<-0.000375049857613607
  tmp_rt_td[code=='166016' & date == as.Date('2015-02-02')]$rt<-0.000944351822492528
  tmp_rt_td[code=='166016' & date == as.Date('2015-07-30')]$rt<-0.000444195589399898
  tmp_rt_td[code=='166016' & date == as.Date('2015-07-31')]$rt<-0.000554179764699958
  tmp_rt_td[code=='166016' & date == as.Date('2016-01-29')]$rt<-0.00045106745531065
  tmp_rt_td[code=='166016' & date == as.Date('2016-02-01')]$rt<--0.000145651883250331
  tmp_rt_td[code=='166016' & date == as.Date('2016-02-02')]$rt<-0.00564923404226114
  tmp_rt_td[code=='F166021' & date %in% as.Date(c('2014-5-27','2014-5-28','2014-5-29','2014-11-27','2014-11-28','2014-12-1','2015-5-27','2015-5-28','2015-5-29','2015-11-27','2015-11-30','2015-12-1','2016-5-27','2016-5-30','2016-12-1'))]$rt<-c(0.000526973760285099,0.000554261432409442,0.000763098390563322,0.000280849325514865,-1.8637270154942E-06,7.67581495579872E-05,-0.000256862288835813,-0.000100101673465858,-0.000434187295041077,0.000708130000000029,0.000611790000000001,0.00094751000000004,0.000303401600000086,0.000337068957248166,-0.00100200400801598)
  
  tmp_rt_ztd[code=='F166010' & date == as.Date('2014-06-17')]$rt<--0.003214470
  tmp_rt_ztd[code=='155039' & date == as.Date('2014-06-17')]$rt<-0
  tmp_rt_ztd[code=='155040' & date == as.Date('2014-06-17')]$rt<--0.01
  
  tmp_rt_ztd[code=='166016' & date == as.Date('2013-07-30')]$rt<-0.000235121413995509
  tmp_rt_ztd[code=='166016' & date == as.Date('2013-07-31')]$rt<--7.65857575039108E-05
  tmp_rt_ztd[code=='166016' & date == as.Date('2014-01-30')]$rt<-0.00055181706813956
  tmp_rt_ztd[code=='166016' & date == as.Date('2014-02-07')]$rt<-0.00248802866360442
  tmp_rt_ztd[code=='166016' & date == as.Date('2014-07-30')]$rt<--0.000331599246851444
  tmp_rt_ztd[code=='166016' & date == as.Date('2014-07-31')]$rt<-0.000162615555370449
  tmp_rt_ztd[code=='166016' & date == as.Date('2015-01-30')]$rt<-0.000375049857613607
  tmp_rt_ztd[code=='166016' & date == as.Date('2015-02-02')]$rt<-0.000944351822492528
  tmp_rt_ztd[code=='166016' & date == as.Date('2015-07-30')]$rt<-0.000444195589399898
  tmp_rt_ztd[code=='166016' & date == as.Date('2015-07-31')]$rt<-0.000554179764699958
  tmp_rt_ztd[code=='166016' & date == as.Date('2016-01-29')]$rt<-0.00045106745531065
  tmp_rt_ztd[code=='166016' & date == as.Date('2016-02-01')]$rt<--0.000145651883250331
  tmp_rt_ztd[code=='166016' & date == as.Date('2016-02-02')]$rt<-0.00564923404226114
  tmp_rt_ztd[code=='F166021' & date %in% as.Date(c('2014-5-27','2014-5-28','2014-5-29','2014-11-27','2014-11-28','2014-12-1','2015-5-27','2015-5-28','2015-5-29','2015-11-27','2015-11-30','2015-12-1','2016-5-27','2016-5-30','2016-12-1'))]$rt<-c(0.000526973760285099,0.000554261432409442,0.000763098390563322,0.000280849325514865,-1.8637270154942E-06,7.67581495579872E-05,-0.000256862288835813,-0.000100101673465858,-0.000434187295041077,0.000708130000000029,0.000611790000000001,0.00094751000000004,0.000303401600000086,0.000337068957248166,-0.00100200400801598)
  
  #货币基金的收益率使用万份收益率/10000,对于定期报告的规则，货币的日期使用自然日而非交易日  
  tmp_rt[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$rt<-fasset[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$treturn/10000
  tmp_rt_td[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$rt<-fasset[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$treturn/10000
  tmp_rt_ztd<-tmp_rt_ztd[!(code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code)]
  tmp_rt_ztd<-rbind(tmp_rt_ztd,tmp_rt[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code])
  tmp_rt_ztd$type<-'ztd'
  
  #指定每个收益率的累计类型，分为sum 和 cum两种
  tmp_rt$cumtype<-'cum'
  tmp_rt_td$cumtype<-'cum'
  tmp_rt_ztd$cumtype<-'cum'
  
  
  #1、读取组合基准对应表
  pbench<-data.table(read.xlsx2(paste(dbadr,'portbench.xlsx',sep=''),1,stringsAsFactors = FALSE))
  pbench$sd<-as.Date(as.numeric(pbench$sd),origin='1899-12-30')
  pbench$ed<-as.Date(as.numeric(pbench$ed),origin='1899-12-30')
  
  
  #生成组合基准的收益率
  #3-2合成基准的指数
  benchr_temp<-merge(pbench,bench_index,by.x='indcode',by.y='indcode',all.x=TRUE,allow.cartesian=TRUE)
  benchr_temp[is.na(rt)]$rt<-0
  temp<-benchr_temp$date
  month(temp)<-12
  day(temp)<-31
  benchr_temp$brtctr<-as.numeric(benchr_temp$wt)*benchr_temp$rt+as.numeric(benchr_temp$value)/yday(temp)
  benchr<-benchr_temp[date>=sd & date<=ed,sum(brtctr),by=c('dbcode','name','method','date')]
  names(benchr)<-c('code','name','method','date','rt')
  benchr<-benchr[date>=std]
  
  tmp<-ztd[date>=std]
  tmp_dl<-data.table(date=wd$date,sd=wd$date,ed=wd$date)
  dl<-tmp_dl[tmp,on=.(sd>preztd,ed<=date)]
  
  fdl<-data.table(code='s',date=as.Date('2009-01-01'),sd=as.Date('2009-01-01'),ed=as.Date('2009-01-01'))[-1]
  for (ii in 1 : length(fcode_dc$code))
  {
    
    if(fcode_dc[ii]$innerclass=='货币型')
    {
      temp<-data.table(code=rep(fcode_dc$code[ii],times=length(wd[date>=fcode_dc$sopdate[ii]]$date)),date=wd[date>=fcode_dc$sopdate[ii]]$date,sd=wd[date>=fcode_dc$sopdate[ii]]$pred,ed=wd[date>=fcode_dc$sopdate[ii]]$date)
      
    }
    else
    {
      temp<-data.table(code=rep(fcode_dc$code[ii],times=length(dl[date>=fcode_dc$sopdate[ii]]$date)),date=dl[date>=fcode_dc$sopdate[ii]]$date,sd=dl[date>=fcode_dc$sopdate[ii]]$sd,ed=dl[date>=fcode_dc$sopdate[ii]]$ed)
    }
    fdl<-rbind(fdl,temp)
  }
  
  
  benchr<-merge(fdl,benchr,by.x=c('code','date'),by.y=c('code','date'),all=TRUE)
  benchr<-benchr[!is.na(ed)]
  benchr<-benchr[order(code,method,date,decreasing=FALSE)]
  benchr[is.na(rt)]$rt<-0
  
  imp<-unique(pbench[,c('code','name','method','sd','ed'),with=FALSE])
  for (ii in 1 : length(imp$dbcode))
  {
    benchr[is.na(method) & date>=imp$sd[ii] & date<= imp$ed[ii] & code==imp$dbcode[ii]]$method<-imp$method[ii]
    benchr[is.na(name) & date>=imp$sd[ii] & date<= imp$ed[ii] & code==imp$dbcode[ii]]$name<-imp$name[ii]
  }
  
  
  benchr_sum<-benchr[method=='sum',sum(rt),by=c('code','ed','name','method')]
  benchr_cum<-benchr[method=='cum',prod(rt+1)-1,by=c('code','ed','name','method')]
  benchr<-rbind(benchr_sum,benchr_cum)
  names(benchr)<-c('code','date','name','method','rt')
  
  #benchr$cumrt<-as.numeric(NA)
  #benchr$cumrt_break<-as.numeric(NA)
  benchr<-benchr[order(code,method,date,decreasing=FALSE)]
  
  names(benchr)<-c('code','date','name','cumtype','rt')
  benchr$type<-'bm'
  benchr<-benchr[,.(code,date,rt,type,cumtype)]
  
  fundrt<-rbind(tmp_rt,tmp_rt_td,tmp_rt_ztd,benchr)  
  dbWriteTable(con, "fundrt",fundrt, overwrite=FALSE, append=TRUE,row.names=F)
  
  
  #生成每日的中证800指数成分及权重信息
  # sqlstr<-str_c('select S_INFO_WINDCODE as indcode, S_CON_WINDCODE as seccode, S_CON_INDATE as ssd, S_CON_OUTDATE as eed from NEWWIND.AIndexMembers where S_INFO_WINDCODE = \'000906.SH\'')
  # indcons<-data.table(sqlQuery(winddb,sqlstr,as.is=TRUE))
  # names(indcons)<-c('indcode','seccode','ssd','eed')
  # indcons$ssd<-as.Date(indcons$ssd,format='%Y%m%d')
  # indcons$eed<-as.Date(indcons$eed,format='%Y%m%d')
  # indcons[is.na(eed)]$eed<-as.Date('2999-12-31')
  
  # #读取股票权数数据
  # sqlstr<-str_c('select S_INFO_WINDCODE,CHANGE_DT,S_INFO_WEIGHTS,S_INFO_INDEX_WEIGHTSRULE from NEWWIND.BOIndexWeightsWIND where  S_INFO_WINDCODE in (select UNIQUE(S_CON_WINDCODE) from NEWWIND.AIndexMembers where S_INFO_WINDCODE = \'000906.SH\' )')
  # indcons_wn<-data.table(sqlQuery(winddb,sqlstr,as.is=TRUE))
  # names(indcons_wn)<-c('seccode','chgd','wn','type')
  # indcons_wn$chgd<-as.Date(indcons_wn$chgd,format='%Y%m%d')
  
  # sqlstr<-str_c('select S_INFO_WINDCODE as seccode,TRADE_DT ,S_DQ_CLOSE as price from NEWWIND.AShareEODPrices where S_INFO_WINDCODE in (select UNIQUE(S_CON_WINDCODE) from NEWWIND.AIndexMembers where S_INFO_WINDCODE = \'000906.SH\' ) and TRADE_DT between \'', format(min(zotd$date),'%Y%m%d') ,'\' and \'',format(max(zotd$date),'%Y%m%d') ,'\'')
  # secprice<-data.table(sqlQuery(winddb,sqlstr,as.is=TRUE))
  # names(secprice)<-c('seccode','date','price')
  # secprice$date<-as.Date(secprice$date,format='%Y%m%d')
  
  
  # tmp_indcon_wt<-merge(indcons,indcons_wn,by='seccode',allow.cartesian=T,all.x=TRUE)
  
  # ds<-unique(zotd[!is.na(pretd)]$date)
  # for (ii in 1 : length(ds))
  # {
  #   tmp<-tmp_indcon_wt[ssd<=ds[ii] & eed>ds[ii] & type==7]
  #   tmp<-merge(,secprice,by=c('seccode'))
  
  # }
  
  
  cons<-dbListConnections(MySQL())
  for(con in cons) 
  {
    dbDisconnect(con)
  }
  gc()
}




update_factor<-function(ed){
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
  
  sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.ASWSIndexEOD where s_info_windcode in (',factorcode,') and trade_dt =\'',ed,'\'',sep='')
  factor_return<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
  names(factor_return)<-c('f_seccode','date','preclose','close')
  factor_return$date<-ymd(factor_return$date)
  factor_return$preclose<-as.numeric(factor_return$preclose)
  factor_return$close<-as.numeric(factor_return$close)
  factor_return$f_return<-log(factor_return$close/factor_return$preclose)
  factor_return<-factor_return[!is.na(factor_return$f_return),]
  
  sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.AIndexEODPrices where s_info_windcode in (',factorcode,') and trade_dt =\'',ed,'\'',sep='')
  A_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
  names(A_factor)<-c('f_seccode','date','preclose','close')
  A_factor$date<-ymd(A_factor$date)
  A_factor$preclose<-as.numeric(A_factor$preclose)
  A_factor$close<-as.numeric(A_factor$close)
  A_factor$f_return<-log(A_factor$close/A_factor$preclose)
  
  #计算超额收益
  factor_return<-merge(factor_return,A_factor[,c('date','f_return')],by.x = 'date',by.y = 'date',all.x = TRUE)
  factor_return$f_return.x<-factor_return$f_return.x-factor_return$f_return.y
  
  factor_return<-factor_return[!is.na(factor_return$f_return.x),]
  factor_return<-as.data.table(factor_return)
  factor_return<-factor_return[,c('f_seccode','date','f_return.x')]
  names(factor_return)<-c('f_seccode','date','f_return')
  
  factor_return<-rbind(factor_return,A_factor[,c('f_seccode','date','f_return')])
  
  sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.HKIndexEODPrices where s_info_windcode in (',factorcode,') and trade_dt =\'',ed,'\'',sep='')
  H_factor<-sqlQuery(windriskdb,sqlstr,as.is=TRUE)
  names(H_factor)<-c('f_seccode','date','preclose','close')
  H_factor$date<-ymd(H_factor$date)
  H_factor$preclose<-as.numeric(H_factor$preclose)
  H_factor$close<-as.numeric(H_factor$close)
  H_factor$f_return<-log(H_factor$close/H_factor$preclose)
  
  factor_return<-rbind(factor_return,H_factor[,c('f_seccode','date','f_return')])
  
  sqlstr<-paste('select s_info_windcode,trade_dt,s_dq_preclose,s_dq_close from newwind.AIndexWindIndustriesEOD where s_info_windcode in (',factorcode,') and trade_dt =\'',ed,'\'',sep='')
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
  
  #修正港股指数的na
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
  
  library(DBI)
  library(RMySQL)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  dbWriteTable(con, "factor_return", factor_return, overwrite=FALSE, append=TRUE,row.names=F)
  
}


#加载组合个券收益率数据生成的函数
data_secr<- function(span,ed)
{
  library(DBI)
  library(RMySQL)
  library(data.table)
  library(WindR)
  w.start()
  library(stringr)
  
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  sec<-data.table(dbGetQuery(con, str_c('SELECT seccode,market FROM rm.fundhold where date = \'', ed, '\'and atype =\'SPT_S\' group by seccode,market')))
  
  num<-length(sec[market!='HK']$seccode)
  sampdate<-as.Date(w.tdaysoffset(-span+1,ed)$Data[[1]])
  #获取sec的样本并计算收益率
  library(RODBC)
  wind=odbcConnect('wind',uid='lianghua',pwd='lianghua',believeNRows=FALSE)
  sqlstr<-str_c('select S_INFO_WINDCODE as seccode,TRADE_DT ,S_DQ_CLOSE as price,S_DQ_PRECLOSE as preprice,S_DQ_TRADESTATUS as status from NEWWIND.AShareEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',sec[market!='HK']$seccode[1:min(1000,num)],'\''),collapse=','), ')  or   S_INFO_WINDCODE in (',str_c(str_c('\'',str_replace_na(sec[market!='HK']$seccode[1000:num]),'\''),collapse=','), ')  and TRADE_DT between \'', format(sampdate,'%Y%m%d') ,'\' and \'',format(ed,'%Y%m%d') ,'\'')
  #if(num<=1000)
  #{
  
  #sqlstr<-str_c('select S_INFO_WINDCODE as seccode,TRADE_DT ,S_DQ_CLOSE as price,S_DQ_PRECLOSE as preprice,S_DQ_TRADESTATUS as status from NEWWIND.AShareEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',sec[market!='HK']$seccode,'\''),collapse=','), ') and TRADE_DT between \'', format(sampdate,'%Y%m%d') ,'\' and \'',format(ed,'%Y%m%d') ,'\'')
  
  #}
  #else
  #{
  
  #sqlstr<-str_c('select S_INFO_WINDCODE as seccode,TRADE_DT ,S_DQ_CLOSE as price,S_DQ_PRECLOSE as preprice,S_DQ_TRADESTATUS as status from NEWWIND.AShareEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',sec[market!='HK']$seccode[1:1000],'\''),collapse=','), ')  or   S_INFO_WINDCODE in (',str_c(str_c('\'',sec[market!='HK']$seccode[1000:num],'\''),collapse=','), ')  and TRADE_DT between \'', format(sampdate,'%Y%m%d') ,'\' and \'',format(ed,'%Y%m%d') ,'\'')
  
  
  #}
  
  secr<-data.table(sqlQuery(wind,sqlstr,as.is=TRUE))
  names(secr)<-c('seccode','date','price','preprice','status')
  secr$price<-as.numeric(secr$price)
  secr$preprice<-as.numeric(secr$preprice)
  secr$date<-as.Date(secr$date,'%Y%m%d')
  secr$ztp<-round(secr$preprice*1.1,2)
  secr$dtp<-round(secr$preprice*0.9,2)
  secr$logr<-log(secr$price/secr$preprice)
  #非交易日的收益率置为NA
  secr[status=='停牌']$logr<-NA
  
  
  #下面对股票上市至涨停打开这段时间的收益率进行处理
  #首先获取新股上市至涨停打开区间的数据
  #找出区间首日涨停的股票
  secr<-secr[order(seccode,date)]
  scode<-unique(secr[price>=ztp,.SD[1],by=.(seccode)]$seccode)
  #筛选股票发行日，上市超过区间首日1个月的，肯定不是新股，筛除
  nscode<-data.table(sqlQuery(wind,str_c('select S_INFO_WINDCODE as seccode,S_INFO_LISTDATE as "ldate" from NEWWIND.AShareDescription where S_INFO_WINDCODE in (',str_c(str_c('\'',sec[market!='HK']$seccode[1:min(1000,num)],'\''),collapse=','), ')  or   S_INFO_WINDCODE in (',str_c(str_c('\'',str_replace_na(sec[market!='HK']$seccode[1000:num]),'\''),collapse=','), ')'),as.is=TRUE))
  nscode$ldate<-as.Date(nscode$ldate,'%Y%m%d')
  
  scode<-data.table(seccode=nscode[ldate>= as.Date(w.tdaysoffset(-30,sampdate)$Data[[1]])][[1]])
  
  ns<-data.table(sqlQuery(wind,str_c('select S_INFO_WINDCODE as seccode,TRADE_DT as "date",S_DQ_CLOSE as price,S_DQ_PRECLOSE as preprice,S_DQ_TRADESTATUS as status from NEWWIND.AShareEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',scode$seccode,'\''),collapse=','), ')'),as.is=TRUE))
  names(ns)<-c('seccode','date','price','preprice','status')
  ns$price<-as.numeric(ns$price)
  ns$preprice<-as.numeric(ns$preprice)
  ns$date<-as.Date(ns$date,'%Y%m%d')
  ns$ztp<-round(ns$preprice*1.1,2)
  ns$dtp<-round(ns$preprice*0.9,2)
  ns$days<-1
  ns$ztdays<-1
  ns<-ns[order(seccode,date)]
  ns[price<ztp]$ztdays<-0
  for (ii in 1 : length(scode$seccode))
  {
    ns[seccode==scode$seccode[ii]]$days<-cumsum(ns[seccode==scode$seccode[ii]]$days)
    ns[seccode==scode$seccode[ii]]$ztdays<-cumsum(ns[seccode==scode$seccode[ii]]$ztdays)
  }
  
  nsd<-ns[days==ztdays,.(zted=max(date)),by=.(seccode)]
  
  
  #取每个股票的所属行业
  swcode<-c('801010.SI','801740.SI','801880.SI','801150.SI','801130.SI','801770.SI','801750.SI','801020.SI','801160.SI','801050.SI','801890.SI','801030.SI','801200.SI','801230.SI','801760.SI','801140.SI','801730.SI','801710.SI','801080.SI','801720.SI','801170.SI','801790.SI','801120.SI','801210.SI','801040.SI','801110.SI','801780.SI','801180.SI')
  sec_industry<-data.table(sqlQuery(wind,str_c('select S_INFO_WINDCODE,S_CON_WINDCODE ,S_CON_INDATE,S_CON_OUTDATE from NEWWIND.SWIndexMembers where S_INFO_WINDCODE in (',str_c(str_c('\'',swcode,'\''),collapse=','), ') and S_CON_WINDCODE in (',str_c(str_c('\'',sec[market!='HK']$seccode[1:min(1000,num)],'\''),collapse=','), ')  or   S_INFO_WINDCODE in (',str_c(str_c('\'',str_replace_na(sec[market!='HK']$seccode[1000:num]),'\''),collapse=','), ')'),as.is=TRUE))
  names(sec_industry)<-c('indcode','seccode','ssd','eed')
  sec_industry$ssd<-as.Date(sec_industry$ssd,'%Y%m%d')
  sec_industry$eed<-as.Date(sec_industry$eed,'%Y%m%d')
  sec_industry<-sec_industry[order(seccode,ssd)]
  sec_industry[is.na(eed)]$eed<-as.Date('2999-12-31')
  #把股票第一次入选行业的起始日调整为1900-01-01。
  sec_industry<-sec_industry[order(seccode,ssd)]
  index<-sec_industry[,.SD[1],by=.(seccode)]
  sec_industry[str_c(seccode,ssd) %in% str_c(index$seccode,index$ssd)]$ssd<-as.Date('1900-01-01')
  sec_industry<-sec_industry[eed>=ed & ssd<=ed]
  
  
  prtmp<-sec_industry[,length(seccode),by=.(seccode)]
  if (length(prtmp[V1>1])>0)
  {
    print('以下股票在同一段时间属于多个行业：')
    print(prtmp[V1>1])
  }
  
  td<-as.Date(w.tdays(sampdate,ed)$Data[[1]])
  sectd<-data.table(seccode='s',date=ed)[-1]
  for (ii in 1 : length(sec[market!='HK']$seccode))
  {
    sectd<-rbind(sectd,data.table(seccode=rep(sec[market!='HK']$seccode[ii],times=length(td)),date=td))
    
  }
  secr_td<-merge(sectd,secr,by=c('seccode','date'),all.x=TRUE)
  
  #将处于新股连续涨停期打开前的收益率置为所属行业指数收益率，若尚未入选行业指数的，置为中证800指数收益率
  secr_td<-merge(secr_td,nsd,by=c('seccode'),all.x=TRUE)
  
  
  #获取各行业指数收益率
  mind<-c('000906.SH')
  indr_sw<-data.table(sqlQuery(wind,str_c('select S_INFO_WINDCODE,TRADE_DT ,S_DQ_PRECLOSE,S_DQ_CLOSE from NEWWIND.ASWSIndexEOD where S_INFO_WINDCODE in (',str_c(str_c('\'',swcode,'\''),collapse=','), ')'),as.is=TRUE))
  names(indr_sw)<-c('indcode','date','preprice','price')
  indr_sw$date<-as.Date(indr_sw$date,'%Y%m%d')
  indr_sw$preprice<-as.numeric(indr_sw$preprice)
  indr_sw$price<-as.numeric(indr_sw$price)
  indr_sw$logir<-log(indr_sw$price/indr_sw$preprice)
  
  indr<-data.table(sqlQuery(wind,str_c('select S_INFO_WINDCODE,TRADE_DT ,S_DQ_PRECLOSE,S_DQ_CLOSE from NEWWIND.AIndexEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',mind,'\''),collapse=','), ')'),as.is=TRUE))
  names(indr)<-c('indcode','date','preprice','price')
  indr$date<-as.Date(indr$date,'%Y%m%d')
  indr$preprice<-as.numeric(indr$preprice)
  indr$price<-as.numeric(indr$price)
  indr$logir<-log(indr$price/indr$preprice)
  
  indr<-rbind(indr,indr_sw)
  
  #分类处理发行前及初期的收益率序列：1、对于有对应行业指数的，小于涨停截止日以及无行情信息的日期，采用行业指数收益率；2、对于无对应行业指数的，小于涨停截止日以及无行情信息的日期，采用中证800收益率
  secr_td<-merge(secr_td,sec_industry,by='seccode',all.x=TRUE,allow.cartesian=TRUE)
  secr_td[is.na(indcode)]$indcode<-'000906.SH'
  secr_td[is.na(ssd)]$ssd<-as.Date('1900-01-01')
  secr_td[is.na(eed)]$eed<-as.Date('2999-12-31')
  #对于连续涨停期打开前的收益率全部使用对应指数收益率
  secr_td<-merge(secr_td,indr[,.(indcode,date,logir)],by=c('indcode','date'),all.x=TRUE)
  secr_td[date<=zted]$logr<-secr_td[date<=zted]$logir
  #对于无行情的日期，使用对应指数收益率
  secr_td[is.na(price)]$logr<-secr_td[is.na(price)]$logir
  secr_td<-secr_td[order(seccode,date)]
  
  #判断当前样本是否有缺失天数是否大于span/2的，若有则使用指数进行填充，直至填充到不少于span/2，然后使用：
  # 
  
  msdata<-secr_td[is.na(logr),length(logr),by='seccode']
  msd<-msdata[V1>=span/2]
  if (length(msd$seccode)!=0)
  {
    for (ii in 1 : length(msd$seccode))
    {
      secr_td[seccode==msd$seccode[ii] & is.na(logr)][1:(msd$V1[ii]-round(span/2))]$logr<-secr_td[seccode==msd$seccode[ii] & is.na(logr)][1:(msd$V1[ii]-round(span/2))]$logir
      
    }
  }
  #对填充完的数据使用
  secr_td[logr==Inf | is.nan(logr)]$logr<-NA
  secr_td<-secr_td[order(seccode,date)]
  
  library(Amelia)
  #为了避免大数据无法进行计算，将secr_td分成四段填充
  seclen<-length(secr_td$seccode)
  len1<-round(seclen/4)-(round(seclen/4) %% span)
  len2<-round(seclen/2)-(round(seclen/2) %% span)
  len3<-round(seclen*3/4)-(round(seclen*3/4) %% span)
  
  a1.out<-amelia(secr_td[1 : len1,.(date,seccode,logr)],ts = 'date',cs='seccode',m=5,intercs=TRUE)$imputations
  gc()
  a2.out<-amelia(secr_td[(len1 +1): len2,.(date,seccode,logr)],ts = 'date',cs='seccode',m=5,intercs=TRUE)$imputations
  gc()
  a3.out<-amelia(secr_td[(len2 +1) : len3,.(date,seccode,logr)],ts = 'date',cs='seccode',m=5,intercs=TRUE)$imputations
  gc()
  a4.out<-amelia(secr_td[(len3 +1) : seclen,.(date,seccode,logr)],ts = 'date',cs='seccode',m=5,intercs=TRUE)$imputations
  gc()
  secr_td$lgrm<-secr_td$logr
  
  secr_td[1 : len1]$lgrm<-(a1.out[[1]]$logr+a1.out[[2]]$logr+a1.out[[3]]$logr+a1.out[[4]]$logr+a1.out[[5]]$logr)/5
  secr_td[(len1+1) : len2]$lgrm<-(a2.out[[1]]$logr+a2.out[[2]]$logr+a2.out[[3]]$logr+a2.out[[4]]$logr+a2.out[[5]]$logr)/5
  secr_td[(len2+1) : len3]$lgrm<-(a3.out[[1]]$logr+a3.out[[2]]$logr+a3.out[[3]]$logr+a3.out[[4]]$logr+a3.out[[5]]$logr)/5
  secr_td[(len3+1) : seclen]$lgrm<-(a4.out[[1]]$logr+a4.out[[2]]$logr+a4.out[[3]]$logr+a4.out[[4]]$logr+a4.out[[5]]$logr)/5
  
  
  secr_td[is.na(logr),]$logr<-secr_td[is.na(logr),]$lgrm
  
  tmp_secr<-secr_td[,.(seccode,date,logr)]
  
  #港股没有涨跌停的问题，单独进行处理
  if(length(sec[market=='HK']$seccode)!=0)
  { 
    secr_hk<-data.table(sqlQuery(wind,str_c('select s_info_windcode as seccode,trade_dt ,S_DQ_CLOSE as price,S_DQ_PRECLOSE as preprice，S_DQ_VOLUME as volume from NEWWIND.HKshareEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',sec[market=='HK']$seccode,'\''),collapse=','), ') and TRADE_DT between \'', format(sampdate,'%Y%m%d') ,'\' and \'',format(ed,'%Y%m%d') ,'\''),as.is=TRUE))
    names(secr_hk)<-c('seccode','date','price','preprice','volume')
    secr_hk$price<-as.numeric(secr_hk$price)
    secr_hk$preprice<-as.numeric(secr_hk$preprice)
    secr_hk$volume<-as.numeric(secr_hk$volume)
    secr_hk$date<-as.Date(secr_hk$date,'%Y%m%d')
    secr_hk$logr<-log(secr_hk$price/secr_hk$preprice)
    secr_hk[volume==0]$logr<-NA
    sectd_hk<-data.table(seccode='s',date=ed)[-1]
    for (ii in 1 : length(sec[market=='HK']$seccode))
    {
      sectd_hk<-rbind(sectd_hk,data.table(seccode=rep(sec[market=='HK']$seccode[ii],times=length(td)),date=td))
    }
    secr_td_hk<-merge(sectd_hk,secr_hk,by=c('seccode','date'),all.x=TRUE)
    
    mind_hk<-c('LG300.WI','HSHKI.HI')
    indr_hk<-data.table(sqlQuery(wind,str_c('select S_INFO_WINDCODE,TRADE_DT ,S_DQ_PRECLOSE,S_DQ_CLOSE from NEWWIND.HKIndexEODPrices where S_INFO_WINDCODE in (',str_c(str_c('\'',mind_hk,'\''),collapse=','), ') union ','select S_INFO_WINDCODE,TRADE_DT ,S_DQ_PRECLOSE,S_DQ_CLOSE from NEWWIND.AIndexWindIndustriesEOD where S_INFO_WINDCODE in (',str_c(str_c('\'',mind_hk,'\''),collapse=','), ')'),as.is=TRUE))
    names(indr_hk)<-c('indcode','date','preprice','price')
    indr_hk$date<-as.Date(indr_hk$date,'%Y%m%d')
    indr_hk$preprice<-as.numeric(indr_hk$preprice)
    indr_hk$price<-as.numeric(indr_hk$price)
    indr_hk$logir<-log(indr_hk$price/indr_hk$preprice)
    
    secr_td_hk$indcode<-'HSHKI.HI'
    secr_td_hk[date<='2016-12-04']$indcode<-'LG300.WI'
    
    
    secr_td_hk<-merge(secr_td_hk,indr_hk[,.(indcode,date,logir)],by=c('indcode','date'),all.x=TRUE)
    secr_td_hk[is.na(price)]$logr<-secr_td_hk[is.na(price)]$logir
    #非港股交易日的数据置为0
    secr_td_hk[is.na(logir)]$logr<-NA
    
    
    msdata_hk<-secr_td_hk[is.na(logr),length(logr),by='seccode']
    msd_hk<-msdata_hk[V1>=span/2]
    if (length(msd_hk$seccode)!=0)
    {
      for (ii in 1 : length(msd_hk$seccode))
      {
        secr_td_hk[seccode==msd_hk$seccode[ii] & is.na(logr)][1:(msd_hk$V1[ii]-round(span/2))]$logr<-secr_td_hk[seccode==msd$seccode[ii] & is.na(logr)][1:(msd_hk$V1[ii]-round(span/2))]$logir
      }
      
    }
    
    #对填充完的数据使用
    secr_td_hk[logr==Inf | is.nan(logr)]$logr<-NA
    a_hk.out<-amelia(secr_td_hk[,.(date,seccode,logr)],ts = 'date',cs='seccode',m=5,intercs=TRUE)$imputations
    secr_td_hk$lgrm<-(a_hk.out[[1]]$logr+a_hk.out[[2]]$logr+a_hk.out[[3]]$logr+a_hk.out[[4]]$logr+a_hk.out[[5]]$logr)/5
    secr_td_hk[is.na(logr),]$logr<-secr_td_hk[is.na(logr),]$lgrm
    
    tmp_secr<-rbind(secr_td[,.(seccode,date,logr)],secr_td_hk[,.(seccode,date,logr)])
  }
  
  
  dbWriteTable(con, "secr_var", tmp_secr, overwrite=FALSE, append=TRUE,row.names=F)
  cons<-dbListConnections(MySQL())
  for(con in cons) 
  {
    dbDisconnect(con)
  }
  gc()
  
}


#EWMA经常需要，所以单独载入
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

#载入生产lambda的函数

matrix_lambda<-function(sd,ed,process_period){
  #连接mysql
  library(DBI)
  library(RMySQL)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  dbSendQuery(con,'SET NAMES gbk')
  selected_index_dt_final<- as.data.table(dbGetQuery(con,str_c('select * from factor_return where date between \'', sd, '\' and \'',ed, '\'')))
  temp <- as.data.table(dbGetQuery(con,str_c('select f_seccode, f_name from factorlist ')))
  selected_index_dt_final<-merge(selected_index_dt_final,temp,by.x = 'f_seccode',by.y = 'f_seccode',all.x = TRUE)
  selected_index_dt_final<-selected_index_dt_final[order(f_seccode,date,decreasing = FALSE),]
  Industry_Index_dataset <- as.data.table(dbGetQuery(con,"select * from factorlist;"))
  Industry_Index_dataset$ssd<-as.Date(Industry_Index_dataset$ssd)
  Industry_Index_dataset$eed<-as.Date(Industry_Index_dataset$ssd)
  
  Industry_Index_dataset <- as.data.table(dbGetQuery(con,"select * from factorlist;"))
  Industry_Index_selected<-Industry_Index_dataset[Industry_Index_dataset$version == version_used & sd > as.Date(Industry_Index_dataset$ssd) & ed < as.Date(Industry_Index_dataset$eed),]$f_seccode  
  selected_index_dt_final<-selected_index_dt_final[f_seccode %in% Industry_Index_selected]
  selected_index_dt_final$date<-as.Date(selected_index_dt_final$date)
  
  #载入生成lambda的函数
  # Lambda optimize
  Lambda.optimize <- function(process_period, selected_index_dt_final){
    ### Period selection
    lambda_data = as.data.table(selected_index_dt_final)
    all_indexes_length = length(unique(lambda_data$f_seccode))
    ### parameter input
    RMSE.period = process_period
    # init.time = proc.time()
    ###
    lambda.range <- seq(0.80, 1, by = 0.01)
    RMSE.return = as.data.table(matrix(numeric(0),nrow = RMSE.period))
    RMSE.var = as.data.table(matrix(numeric(0),nrow = RMSE.period))
    RMSE = as.data.frame(matrix(numeric(0),nrow = length(lambda.range)))
    RMSE.cov = as.data.frame(matrix(numeric(0),nrow = length(lambda.range)))
    temp_coreturn_df = as.data.table(matrix(numeric(0),nrow=c(RMSE.period)))
    temp_cov_df = as.data.table(matrix(numeric(0),nrow=c(process_period+1)))
    
    for (l in (1:length(lambda.range))){
      
      # l = 1
      # lambda_data$f_return = 0
      lambda_data$RVar_T1 = 0
      # cov_df = data.frame(matrix(numeric(0),nrow=c(total_days-review_period+1)))
      name_list <- unique(lambda_data$f_seccode)
      length_name <- all_indexes_length
      temp_lambda = lambda.range[l]
      for (j in (1:length_name)){
        # j = 2
        data_temp = lambda_data[f_seccode %in% name_list[j]]
        data_temp_return = data_temp$f_return
        
        for (d in (1:review_period)){
          data_temp$RVar_T1[review_period+d] = EWMA(temp_lambda,data_temp_return[d:c(d+review_period-1)])
        }
        lambda_data[f_seccode %in% name_list[j]] = data_temp
        # lambda_data[f_seccode %in% name_list[j]][process_start_day:570]
        # Calculate the covariance
        # time0 = proc.time()
        if (j > 1){
          for (m in (c(j-1):1)){
            # temp_name = paste(as.character(name_list[j]),as.character(name_list[m]),sep ="_")
            temp_list = c()
            lambda_data_return = lambda_data[f_seccode %in% name_list[m]]$f_return
            for (z in (1:c(process_period+1))){
              # temp_list = Covar(temp_lambda, data_temp_return[z:c(review_period+z-1)],lambda_data_return[z:c(review_period+z-1)])
              temp_list = rbind(temp_list, Covar(temp_lambda, data_temp_return[z:c(review_period+z-1)],lambda_data_return[z:c(review_period+z-1)]))
            }
            temp_cov_df[[paste(as.character(name_list[j]),as.character(name_list[m]),sep ="_")]] = temp_list
          }
        }
        # print(proc.time() - time0)
      }
      # proc.time() - time1
      
      final_data_temp = lambda_data[, .SD[c(process_start_day):total_days],by = f_seccode]
      index.var.list = final_data_temp[, .SD[1:c(process_period)], by = f_seccode]$RVar_T1 # Consider time t forecast rather than time t+1
      final_cov_df = temp_cov_df[1:process_period,] # 286 dates include t0 and tT+1, we need t1 to tT ##pick up [1:process_period,] or [2:process_period+1,] here
      
      # Calculate co-return to calculate RMSE
      for (a in (1:length(name_list))){ # 
        # names(RMSE.return)[length(RMSE.return)] = as.character(name_list[a]) 
        # a =2 
        RMSE.return[[as.character(name_list[a])]] = final_data_temp[f_seccode == name_list[a]]$f_return
        # RMSE.return[["801890.SI"]]
        # index.return.list[c(1+RMSE.period*(a-1)):c(RMSE.period*a)]
        if (a > 1){
          for (n in (c(a-1):1)){
            temp_coreturn_df[[paste(as.character(name_list[a]),as.character(name_list[n]),sep="_")]] = RMSE.return[[as.character(name_list[a])]] * RMSE.return[[name_list[n]]]
          }
        } 
        # names(RMSE.var)[length(RMSE.var)] = as.character(name_list[a])
        RMSE.var[[as.character(name_list[a])]] = index.var.list[c(1+RMSE.period*(a-1)):c(RMSE.period*a)]
        # RMSE.var[["801890.SI"]]
        error = sqrt(sum((RMSE.return[[a]]**2 - RMSE.var[[a]])**2)/RMSE.period)
        RMSE[l,a] = error # "l" states different lambda, "a" states different indexes
      }
      if (length(colnames(temp_coreturn_df))>0){
        for (c in (1:length(colnames(temp_coreturn_df)))){
          RMSE.cov[l,c] = sqrt(sum((temp_coreturn_df[[c]] - final_cov_df[[c]])**2)/RMSE.period)
        }
      }
      
    }
    colnames(RMSE) = name_list
    colnames(RMSE.cov) = colnames(temp_coreturn_df) 
    # RMSE
    lambda.var.list =c()
    lambda.cov.list =c()
    
    for (b in (1:length(name_list))){
      
      temp_var_index= which(RMSE[[b]] == min(RMSE[[b]]))
      lambda.var.list=cbind(lambda.var.list, lambda.range[temp_var_index])
    }
    lambda.var.vector = t(lambda.var.list)
    rownames(lambda.var.vector) = colnames(RMSE)
    
    if (length(colnames(RMSE.cov))>0){
      for (k in (1:length(colnames(RMSE.cov)))){
        temp_cov_index= which(RMSE.cov[,k] == min(RMSE.cov[,k]))
        lambda.cov.list=cbind(lambda.cov.list, lambda.range[temp_cov_index])
      }
    }
    if (is.null(lambda.cov.list) == F){
      lambda.cov.list <- as.numeric(lambda.cov.list)
    }
    lambda.cov.vector = t(t(lambda.cov.list))
    rownames(lambda.cov.vector) = colnames(RMSE.cov)
    
    print(lambda.var.vector)
    print(lambda.cov.vector)
    final_list = list(lambda.var.vector = lambda.var.vector, lambda.cov.vector = lambda.cov.vector)
    return(final_list)
  }
  
  all_lambda<-Lambda.optimize(process_period, selected_index_dt_final)
  #all_lambda是一个list,所以需要分别存储
  temp_cov<-all_lambda$lambda.cov.vector
  cov<- data.table(name = rownames(temp_cov),lambda=temp_cov)
  names(cov)<-c('name','lambda')
  cov$factor_1<-NA
  cov$factor_2<-NA
  cov$name<-as.character(cov$name)
  for(ii in 1:length(cov$name)){
    print(ii)
    cov$factor_1[ii]<-substr(cov$name[ii],1,(str_locate(cov$name[ii],'_')-1))
    cov$factor_2[ii]<-substr(cov$name[ii],(str_locate(cov$name[ii],'_')+1),nchar(cov$name[1]))
  }
  cov<-cov[,c('factor_1','factor_2','lambda')]
  cov$ssd<-ed
  cov$eed<-w.tdaysoffset(1,ed)$Data[[1]]
  
  temp_var<-all_lambda$lambda.var.vector
  var<- data.table(name = rownames(temp_var),lambda=temp_var)
  names(var)<-c('factor_1','lambda')
  var$factor_2<-var$factor_1
  var<-var[,c('factor_1','factor_2','lambda')]
  var$ssd<-ed
  var$eed<-w.tdaysoffset(1,ed)$Data[[1]]
  
  lambda<-rbind(cov,var)
  return(lambda)
  cons<-dbListConnections(MySQL())
  for(con in cons) 
  {
    dbDisconnect(con)
  }
}



#载入生成协方差矩阵的函数
cov_store<-function(ed,sd,version_used,basic.index,review_period,process_start_day,total_days){
  
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
  lambda_dataset<-as.data.table(dbGetQuery(con,str_c('select * from lambda where ssd=\'',ed,'\'',sep='')))
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
    ###
    # index_dt_final
    # 格式：data table
    # 列变量：因子代码 f_seccode, 日期序列 date, 因子收益率 f_return, 因子所属类别 f_type, 版本 version, 因子中文名 f_name
    # 使用说明: 选取的区间长度默认为570天, 根据global变量total_days决定
    
    # process_period
    # 格式: integer 
    # 使用说明：默认285天，我们假设EWMA非递归算法达到高精确度需要285天，而总数据量取了570天，所以process_period的取值范围是[1,285]。
    
    # cov_factor_matrix
    # 格式：matrix, （30*30对于市场因子）
    # 使用说明：因子之间的lambda构成的矩阵
    ###
    
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
    for (j in (1:length_name)){
      # j = 30
      data_temp = data[f_name == name_list[j]]
      ## Calculate the first day variance in process period
      day1_ewma = EWMA(lambda.var.list[j],data_temp$f_return)
      data_temp$RVar_T1[review_period] = day1_ewma
      data_temp_return = data_temp$f_return
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
          # cov_df[column_name][1,] = Covar(cov_factor_matrix[j,m], data_temp$f_return,data[data$f_name == name_list[m]]$f_return)
          number_temp_list = c()
          lambda_temp_return = data[f_name == name_list[m]]$f_return
          
          for (z in (1:c(total_days-review_period+1))){
            number_temp_list = rbind(number_temp_list,Covar(cov_factor_matrix[j,m], data_temp_return[z:c(review_period+z-1)],lambda_temp_return[z:c(review_period+z-1)]))
          }
          cov_df[,column_name] = number_temp_list
          
        }
      }
      # proc.time() - time1
      # Use the recursive formula to calculate variance every day
      
      for (d in (1:review_period)){
        data_temp$RVar_T1[review_period+d] = EWMA(lambda.var.list[j],data_temp_return[d:c(d+review_period-1)])
      }
      data[f_name == name_list[j]] = data_temp
    }
    
    # proc.time() - ptm
    # full_data <- data
    cov.list <- data[, .SD[total_days],by = f_name]$RVar_T1
    # cov_df[285,]
    # cov.list <- final_data[, .SD[c(total_days-review_period)], by = f_name]$RVar_T1
    # cov_df <- cov_df
    
    left_length = process_period
    # Construct the covariance matrix
    cov.mat = matrix(0,all_indexes_length, all_indexes_length)
    cov.mat[1,1] = cov.list[1]
    for (i in (2:length_name)){
      cov.mat[i,i] = cov.list[i]
      for (j in (c(i-1):1)){
        column_name = paste("cov",i,j,sep ="")
        cov.mat[i,j] = cov.mat[j,i] = cov_df[column_name][left_length,]
      }
    }
    colnames(cov.mat) = name_list
    rownames(cov.mat) = name_list
    return(cov.mat)
  }
  
  #生成协方差矩阵
  all_cov<-Index.var.cov.calculation(index_dt_final, process_period, cov_factor_matrix)
  
  #存储生成的all_cov的矩阵
  matrix_cov<-data.table(factor1='S',factor2='S',cov=0,ssd=as.Date('1900-12-30'),eed=as.Date('1900-12-30'))[-1]
  for(ii in 1:length(colnames(all_cov))){
    print(ii)
    temp<-data.table(factor1=rep(colnames(all_cov)[ii],times=length(rownames(all_cov))),factor2=rownames(all_cov),cov=all_cov[,ii],ssd=rep(ed,times=length(rownames(all_cov))),eed=rep(ed+1,times=length(rownames(all_cov))))
    matrix_cov<-rbind(matrix_cov,temp)
  }
  for(ii in 1:length(Industry_Index_dataset$f_name)){
    print(ii)
    matrix_cov[matrix_cov$factor1==Industry_Index_dataset$f_name[ii]]$factor1<-Industry_Index_dataset$f_seccode[ii]
    matrix_cov[matrix_cov$factor2==Industry_Index_dataset$f_name[ii]]$factor2<-Industry_Index_dataset$f_seccode[ii]
  }
  
  return(matrix_cov)
  cons<-dbListConnections(MySQL())
  for(con in cons) 
  {
    dbDisconnect(con)
  }
}



#载入生成因子暴露度的函数
#连接mysql
cof_stor<-function(lsd,ed,process_period){
  library(DBI)
  library(RMySQL)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="guhao",password="12345678")
  
  #读取指定日期的指数收益率数据，并从中选出有效期内的指数因子
  dbSendQuery(con,'SET NAMES gbk')
  final_data_equity <- as.data.table(dbGetQuery(con,str_c('select * from secr_var where date between \'', lsd, '\' and \'',ed, '\'')))
  temp_industry<-as.data.table(dbGetQuery(con,str_c('select * from stock_industry where out_date>\'',ed,'\' and in_date<=\'',ed,'\'',sep='')))
  final_data_equity<-merge(final_data_equity,temp_industry[,c('seccode','industry')],by.x = 'seccode',by.y = 'seccode',all.x = TRUE)
  names(final_data_equity)<-c('seccode','date','logr','factor_details')
  
  #读取当天的组合全部持仓个券
  temp_hold<-as.data.table(dbGetQuery(con,str_c('select date, seccode from fundhold where date= \'', ed, '\'' )))
  final_data_equity<-final_data_equity[seccode %in% unique(temp_hold$seccode)]
  
  process_index_dt_final<-as.data.table(dbGetQuery(con,str_c('select * from factor_return where date between \'', lsd, '\' and \'',ed, '\'')))
  temp <- as.data.table(dbGetQuery(con,str_c('select f_seccode, f_name from factorlist ')))
  process_index_dt_final<-merge(process_index_dt_final,temp,by.x = 'f_seccode',by.y = 'f_seccode',all.x = TRUE)
  process_index_dt_final<-process_index_dt_final[order(f_seccode,date,decreasing = FALSE),]
  Industry_Index_dataset <- as.data.table(dbGetQuery(con,"select * from factorlist;"))
  Industry_Index_selected<-Industry_Index_dataset[Industry_Index_dataset$version == version_used & sd > as.Date(Industry_Index_dataset$ssd) & ed < as.Date(Industry_Index_dataset$eed),]$f_seccode  
  process_index_dt_final<-process_index_dt_final[f_seccode %in% Industry_Index_selected]
  process_index_dt_final$f_name<-gsub("\\(申万\\)","",process_index_dt_final$f_name)
  
  A_share_indexes_length<-length(unique(Industry_Index_dataset[f_market=='A']$f_seccode))
  all_indexes_length<-length(Industry_Index_selected)
  
  regression <- function(process_period, process_index_dt_final, final_data_equity){
    ###
    # process_index_dt_final
    # 格式：data table
    # 列变量：因子代码 f_seccode, 日期序列 date, 因子收益率 f_return, 因子所属类别 f_type, 版本 version, 因子中文名 f_name
    # 使用说明: 时间序列长度和process_period一致，默认为285天
    
    # process_period
    # 格式: integer 
    # 使用说明：默认285天，我们假设EWMA非递归算法达到高精确度需要285天，而总数据量取了570天，所以process_period的取值范围是[1,285]。
    
    # cov_factor_matrix
    # 格式：matrix, （30*30对于市场因子）
    # 使用说明：因子之间的lambda构成的矩阵
    ###
    
    # regression(process_period, process_index_dt_final, final_data_equity_matrix, final_data_equity)
    # Construct logreturn series for indexes and equities
    # process_index_dt_final = process_index_dt_final
    # # total_days
    # # selected_index_dt_final
    # # full_equity_df
    # final_data_equity = final_data_equity_copy
    # 
    final_data_equity_matrix = data.frame(matrix(final_data_equity$logr, ncol = length(unique(final_data_equity$seccode))))
    colnames(final_data_equity_matrix) = unique(final_data_equity$seccode)
    index_df = data.frame(matrix(numeric(0),nrow=c(process_period)))
    name_list = unique(process_index_dt_final$f_name)
    for (i in (1:length(name_list))){
      # loop parameter input
      index_df[paste(name_list[i])] = process_index_dt_final[f_name==name_list[i]]$f_return
    }
    
    # colnames(index_df) = name_list
    index_m = as.matrix(index_df)
    equity_df = final_data_equity_matrix
    ### The name list and name length after delete some equities
    # m_equity_name_list <- unique(final_data_equity[final_data_equity$f_name %in% colnames(equity_df)]$f_name)
    m_equity_ticker_list <- colnames(equity_df)
    m_equity_length_name <- length(colnames(equity_df))
    ##
    coef_df = data.frame(matrix(nrow =length(name_list), ncol = m_equity_length_name))
    colnames(coef_df) = colnames(equity_df)
    rownames(coef_df) = colnames(index_m)
    coef_df[is.na(coef_df)] <- 0
    # x = 219
    
    my_lms = sapply(1:m_equity_length_name, function(x) lm(if (!str_detect(m_equity_ticker_list[x],"HK")){if(length(index_m[,unique(final_data_equity[final_data_equity$seccode == m_equity_ticker_list[x] & !is.na(final_data_equity$factor_details),]$factor_details)]) != 0)
    {c(equity_df[,x] - index_m[,unique(final_data_equity[final_data_equity$seccode == m_equity_ticker_list[x] & !is.na(final_data_equity$factor_details),]$factor_details)]) ~
        index_m[,1:length(basic.index)] - 1}else{equity_df[,x]~index_m[,1:length(basic.index)] - 1}
      # unique(final_data_equity[seccode==colnames(equity_df)[x]]$factor_details)
    }else{
      equity_df[,x] ~ index_m[,c(A_share_indexes_length+1):all_indexes_length] -1
    }, 
    na.action = na.omit)$coef)  # Since the last day return is always 0
    # if (!str_detect(m_equity_ticker_list[x],"HK")){
    #   index_m[,1:A_share_indexes_length]
    #   }else{index_m[,c(A_share_indexes_length+1):all_indexes_length]}
    H_shares_number = which(!str_detect(m_equity_ticker_list,"HK") == F)
    for (reg.number in 1:m_equity_length_name){
      if (!reg.number %in% H_shares_number){
        # reg.number = 16
        coef_df[1:length(basic.index),reg.number] = t(t(as.numeric(my_lms[reg.number])[1:length(basic.index)]))  # my_lms[,reg.number]
        coef_df[unique(final_data_equity[final_data_equity$seccode == m_equity_ticker_list[reg.number] & !is.na(final_data_equity$factor_details),]$factor_details),reg.number] = 1
      } else {
        coef_df[c(A_share_indexes_length+1):all_indexes_length,reg.number] = as.numeric(my_lms[reg.number])
      }
    }
    #m_equity_name_list = m_equity_name_list,
    return(coef_df)
  }
  
  cof<-regression(process_period, process_index_dt_final, final_data_equity)
  matrix_cof<-data.table(seccode='0001.SZ',factor='I',coefficient=1,ssd=as.Date('1899-12-30'),eed=as.Date('1899-12-30'))[-1]
  for(ii in 1:length(colnames(cof))){
    print(ii)
    temp_cof<-data.table(seccode=rep(colnames(cof)[ii],times=length(rownames(cof))),factor=rownames(cof),coefficient=cof[,ii],ssd=rep(ed,times=length(rownames(cof))),eed=rep(w.tdaysoffset(1,ed)$Data[[1]],times=length(rownames(cof))))
    matrix_cof<-rbind(matrix_cof,temp_cof)
  }
  
  temp_name<-Industry_Index_dataset[,c('f_seccode','f_name')]
  temp_name$f_name<-gsub("\\(申万\\)","",temp_name$f_name)
  matrix_cof<-merge(matrix_cof,temp_name,by.x = 'factor',by.y = 'f_name',all.x = TRUE)
  matrix_cof[factor=='中证800']$f_seccode<-'000906.SH'
  matrix_cof$factor<-matrix_cof$f_seccode
  matrix_cof<-matrix_cof[,c('seccode','factor','coefficient','ssd','eed')]
  return(matrix_cof)
  cons<-dbListConnections(MySQL())
  for(con in cons) 
  {
    dbDisconnect(con)
  }
}


