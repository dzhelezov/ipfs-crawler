

library(dplyr)
library(tibbletime)
library(lubridate)
library(zoo)
library(ggplot2)

########################
# Rough but quick estimates of the network size using the random queries log 
########################

qdata <- read.csv("random_queries.log", header = FALSE)
q_tbl <- tbl_df(qdata)
names(q_tbl) <- c("ts", "pid", "key", "plen", "size")

q_tbl <- q_tbl %>% mutate (
  ts = as.POSIXct(ts,origin="1970-01-01")
)

# histogram of the common prefix len distribution
hist(q_tbl$plen)

## here we first take the median of the prefix length within each 10-min interval,
## next take the rolling mean in a 5-step window (thus spanning 50-min intervals)
q_summ <- q_tbl%>%
            group_by(key) %>%
            filter(plen == max(plen)) %>%
            mutate(minute=minute(ts) %/% 10, hour=hour(ts), day=day(ts)) %>%
            group_by(minute, hour, day) %>%
            summarize(avg_plen=median(plen), time=max(ts)) %>%
            ungroup() %>%
            arrange(time) %>%
            mutate(avg_plen=rollmean(avg_plen, 5, na.pad=TRUE)) %>%
            mutate(size=2^avg_plen)

q_plot <- ggplot(q_summ, aes(x=time, y=size)) +
  geom_line() + 
  xlab("")
q_plot

########################
# Estimating the network size using the connection log 
########################
conndata <- read.csv("conn.log", header=FALSE)
c_tbl <- tbl_df(conndata)

names(c_tbl) <- c("ts", "pid")

c_tbl <- c_tbl %>% mutate (
  date = as.POSIXct(ts,origin="1970-01-01")
)

## this is accurate, but slow
count_window <- function(df, curr_ts, sec_offset){
  summ <-  df %>% filter(ts >= curr_ts - sec_offset, ts <= curr_ts)
  return(n_distinct(summ$pid))
}

v_count_window <- Vectorize(count_window, vectorize.args = c("curr_ts"))

#takes quite a bit of time to evaluate
#c_summ <- c_tbl%>%
#          mutate(sliding_count = v_count_window(., ts, 60*60*2)) #two hours
#
#c_summ_p <- ggplot(c_summ, aes(x=date, y=sliding_count)) +
#  geom_line() + 
#  xlab("")


## this is less accurate but quick; just count unique PIDs every 30 minutes
c_grp <- c_tbl%>%
        mutate(minute=minute(date) %/% 30, hour=hour(date), day=day(date)) %>%
        group_by(minute, hour, day) %>%
        summarize(peer_count=n_distinct(pid), time = max(ts), date=max(date)) %>%
        ungroup() %>%
        arrange(day, hour)

c_grp_p <- ggplot(head(c_grp,-1), aes(x=date, y=peer_count)) +
  geom_line() + 
  xlab("")

c_grp_p