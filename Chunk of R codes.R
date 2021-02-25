library(tidyverse)
library(ggplot2)

#Data importing and altering data types
setwd('C:/Users/Tan/Desktop/kitty/aiven assignment')

#import accounts dataset
accounts <- read.csv('accounts.csv')

accounts$account_created_time <- as.POSIXct(accounts$account_created_time, format = "%Y-%m-%d %H:%M:%S")
accounts$is_deleted <-  as.logical(accounts$is_deleted)
accounts$main_industry <- as.factor(accounts$main_industry)
accounts$sub_industry <- as.factor(accounts$sub_industry)
accounts$type <- as.factor(accounts$type)
accounts$support_tier <- as.factor(accounts$support_tier)

str(accounts)
summary(accounts)

#delete 'continent' and 'country' columns since there is no values in 2 variables

accounts <- accounts[,-c(4,5)]

#Find duplicate: there is no duplicate

duplicate <-accounts[duplicated(accounts),]

duplicate

#Count number of distinct account id in each table

length( unique(accounts$account_id))


#Add column for data analytics:

accounts$month_yr <- as.factor(format(accounts$account_created_time, '%Y-%m'))
accounts$hour <- as.factor(format(accounts$account_created_time,'%H'))
accounts$account_created_date <- as.factor(format(accounts$account_created_time, '%Y-%m-%d'))

summary(accounts)  

#outlier:

accounts %>%
  group_by(month_yr) %>%
  summarise(number_of_accounts = n_distinct(account_id))

accounts %>%
  filter(month_yr=='2020-03') %>%
  group_by(account_created_date) %>%
  summarise(number_of_accounts = n_distinct(account_id))

accounts %>%
  filter(account_created_date=='2020-03-12') %>%
  group_by(hour) %>%
  summarise(number_of_accounts = n_distinct(account_id))

#number of accounts created based on customer types
accounts %>% 
  group_by(type) %>%
  summarise(number_of_users = n_distinct(account_id)) %>%
  ggplot(aes(x=type, y=number_of_users)) +
  geom_bar(stat="identity",width = 0.7) +
  theme(axis.text = element_text(size = 9)) +
  geom_text(aes(label=number_of_users), vjust=-0.3, size=3.5) +
  ggtitle("Number of new accounts created by customer types") +
  labs(y ="Number of new accounts created", x = "Customer Types" )


# Delete account
summary(accounts)
accounts %>% 
  group_by(type) %>%
  filter(is_deleted == 'TRUE') %>%
  summarise(number_of_users = n_distinct(account_id)) %>%
  ggplot(aes(x=type,y=number_of_users)) +
  geom_bar(stat ="identity")+
  geom_text(aes(label=number_of_users), vjust=-0.3, size=3.5)+
  ggtitle("Number of deleted accounts based on type of customer") +
  labs(y = "Number of deleted accounts", x = "Customer Type")


# account with basic support tier
basic_support_tier <- accounts %>%
  select(account_id,account_created_date,hour,is_deleted,type,support_tier) %>%
  filter(support_tier == 'Basic')

#what type of customer need basic support
basic_support_tier %>% 
  group_by(type) %>%
  summarise(number_of_users = n_distinct(account_id)) %>%
  ggplot(aes(x = type, y = number_of_users)) +
  geom_bar(stat="identity") +
  geom_text(aes(label=number_of_users), vjust=-0.3, size=3.5)+
  ggtitle("Number of accounts from different customer types that needed basic support") +
  labs(y ="Number of new accounts created that needed Basic support", x = "Customer Types" )

#delete the account created in 12th of March 2020
accounts <- accounts %>%
  filter(!(account_created_date == '2020-03-12'))

#number of account created in each month
accounts %>% 
  group_by(month_yr) %>%
  summarise(number_of_accounts = n_distinct(account_id)) %>%
  ggplot(aes(x=month_yr, y=number_of_accounts, group = 1)) +
  geom_line( color="black") + 
  geom_point() +
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5) +
  ggtitle("Monthly new accounts created") +
  labs(y = "Number of new accounts created", x = "Month of the year")

#number of account created based on hour
accounts %>% 
  group_by(hour) %>%
  summarise(number_of_accounts = n_distinct(account_id)) %>%
  ggplot(aes(x=hour, y=number_of_accounts, group = 1)) +
  geom_line( color="black") + 
  geom_point() +
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5) +
  ggtitle("Number of accounts created based on time of the day") +
  labs(y = "Number of new accounts created", x = "Time of the day")


#Data importing for account metrics 

account_metrics <- read.csv('account_metrics.csv')

account_metrics$latest_service_type <- as.factor(account_metrics$latest_service_type)
account_metrics$latest_service_created <- as.POSIXct(account_metrics$latest_service_created, format = "%Y-%m-%d %H:%M:%S")
account_metrics$month_yr <- as.factor(format(account_metrics$latest_service_created, '%Y-%m'))
account_metrics$latest_service_created_year <- as.factor(format(account_metrics$latest_service_created, '%Y'))

account_metrics <- account_metrics %>%
  rename(
    kafka = active_kafka_services,
    pg = active_pg_services,
    elasticsearch = active_elasticsearch_services,
    redis = active_redis_services,
    mysql = active_mysql_services,
    grafana = active_grafana_services,
    influxdb = active_influxdb_services,
    m3 = active_m3_services,
    ksql = active_ksql_services,
    kafka_connect = active_kafka_connect_services,
    cassandra = active_cassandra_services,
    kafka_mirrormaker = active_kafka_mirrormaker_services
  )

#check for NA values
summary(account_metrics)
str(account_metrics)

#Find duplicate: there is no duplicate
account_metrics[duplicated(account_metrics),]

#Detect outliers
summary(account_metrics)
boxplot(account_metrics$all_projects)

#change replace faulty json key
account_metrics$clouds_in_use_names[account_metrics$clouds_in_use_names == '{"v":]}'] <- '{"v":[{"v":"NA"}]}'
account_metrics$cloud_regions_in_use_names[account_metrics$cloud_regions_in_use_names == '{"v":]}'] <- '{"v":[{"v":"NA"}]}'

#recheck the accuracy of the observations. All active projects should be the the sum of gcp, aws, azure, do, upcloud, custom do, and packet projects. All active services should be the sum of all active product services.
account_metrics <- account_metrics %>%
                    select_all() %>%
                    mutate(check1=active_projects-gcp_projects-aws_projects-azure_projects-do_projects-upcloud_projects-custom_do_projects-packet_projects) %>%
                    mutate(check2=all_services - active_services - stopped_services) %>%
                    mutate(check3=active_services-kafka-pg-elasticsearch-redis-mysql-grafana-influxdb-m3-ksql-kafka_connect-cassandra-kafka_mirrormaker)
#filter and delete all the rows with inaccurate number
account_metrics <- account_metrics %>%
                      filter(!(check1 != 0|check2 != 0| check3 != 0))

#calculate percentage of active projects and active services
account_metrics <- account_metrics %>%
  mutate(percentage_of_active_project = active_projects/all_projects * 100) %>%
  mutate(percentage_of_active_service = active_services/all_services * 100)
summary(account_metrics)

#number of accounts group by the percentage of active project
account_metrics$groups_percentage_active_project <- cut(account_metrics$percentage_of_active_project, c(0,25,50,75,100))
levels(account_metrics$groups_percentage_active_project) = c("0-25", "25-50", "50-75", "75-100")

account_metrics %>%
  group_by(groups_percentage_active_project)%>%
  drop_na(groups_percentage_active_project) %>%
  summarise(number_of_accounts = n_distinct(account_id)) %>%
  ggplot(aes(x=groups_percentage_active_project, y=number_of_accounts)) +
  geom_bar(stat="identity") +
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5) +
  ggtitle("Number of active accounts group by the percentage of their active projects") +
  labs(y = "Number of active accounts", x= "Percentage of active project compared with all of their project (%)")

#number of accounts group by the percentage of active service
account_metrics$groups_percentage_active_service <- cut(account_metrics$percentage_of_active_service, c(0,25,50,75,100))
levels(account_metrics$groups_percentage_active_service) = c("0-25", "25-50", "50-75", "75-100")

account_metrics %>%
  group_by(groups_percentage_active_service)%>%
  drop_na(groups_percentage_active_service) %>%
  summarise(number_of_accounts = n_distinct(account_id)) %>%
  ggplot(aes(x=groups_percentage_active_service, y=number_of_accounts)) +
  geom_bar(stat="identity") +
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5) +
  ggtitle("Number of active accounts group by the percentage of their active services") +
  labs(y = "Number of active accounts", x= "Percentage of active services compared with all of their services (%)")



#Number of active account based on their latest created service year
account_metrics %>%
  filter(active_projects > 0 | active_services > 0) %>%
  summarise(number_of_accounts = n_distinct(account_id))

account_metrics %>%
  filter(active_projects > 0 | active_services > 0) %>%
  group_by(latest_service_created_year)%>%
  summarise(number_of_accounts = n_distinct(account_id))%>%
  ggplot(aes(x=latest_service_created_year, y=number_of_accounts, group = 1)) +
  geom_line( color="black") + 
  geom_point() +
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5) +
  ggtitle("Number of active account based on their latest created service year") +
  labs(y = "Number of active accounts", x = "Year of the latest service created")

#Number of active accounts of each service
services <- gather(subset(account_metrics, select = c(7:18)) ,key = "key", value = "value", 1:12) 
  
services %>% 
  filter(value > 0) %>%
  group_by(key) %>%
  summarise(number_of_accounts = n()) %>%
  ggplot(aes(x=key, y=number_of_accounts)) +
  geom_bar(stat="identity") +
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5) +
  ggtitle("Number of active accounts of each service product") +
  labs(y = "Number of active accounts", x= "Service Products")

summary(services)


#handling JSON clouds_in_use_names column
library(jsonlite)

cloudjson <- account_metrics$clouds_in_use_names

cloud_name <- data.frame(account_metrics$account_id, account_metrics$clouds_in_use_names)

cloud_name <- cloud_name %>% 
                mutate(cloudjson  = map(cloudjson , ~ fromJSON(.) %>% as.data.frame())) %>% 
                unnest(cloudjson ) %>%
                rename(
                  account_id = account_metrics.account_id,
                  cloud_name = v) 
cloud_name <- cloud_name[,-2]

#number of account used each cloud

cloud_name %>% 
  group_by(cloud_name) %>%
  summarise(number_of_users = n_distinct(account_id))%>%
  drop_na(cloud_name) %>%
  ggplot(aes(x = cloud_name, y = number_of_users)) +
  geom_bar(stat ="identity")+
  geom_text(aes(label=number_of_users), vjust=-0.3, size=3.5)+
  ggtitle("Number of active accounts of each cloud") +
  labs(y = "Number of active accounts", x = "Cloud name")

#handling JSON cloud_regions_in_use_names column
regionjson <- account_metrics$cloud_regions_in_use_names

cloud_region <- data.frame(account_metrics$account_id, account_metrics$cloud_regions_in_use_names)

cloud_region <- cloud_region %>% 
  mutate(regionjson = map(regionjson, ~ fromJSON(.) %>% as.data.frame())) %>% 
  unnest(regionjson) %>%
  rename(
    account_id = account_metrics.account_id,
    region_name = v)
cloud_region <- cloud_region[,-2]

#calculate distinct users of each region

region_users_count <- cloud_region %>% 
                        group_by(region_name) %>%
                        summarise(number_of_accounts = n_distinct(account_id))


region_users_count %>%
  mutate(regions = case_when(
    region_name %in% c("ap-northeast-1","ap-south-1","asia-east1","asia-northeast1","asia-southeast1","asia-southeast2") ~ "Asia Pacific",
    region_name %in% c("Central US","East US","northamerica-northeast1","nyc","sfo","southamerica-east1","us-central1","us-east-1","us-east-2","us-east1","us-east4","us-west-2","us-west1","us-west2","us-west4","sa-east-1") ~ "America",
    region_name %in% c("eu-central-1","eu-north-1","eu-west-1","eu-west-2","europe-north1","europe-west1","europe-west2","europe-west3","europe-west4","europe-west6","West Europe") ~ "Europe",
    TRUE ~ NA_character_
  )) %>%
  drop_na(region_name) %>%
  group_by(regions) %>%
  summarise(number_of_accounts = sum(number_of_accounts))%>%
  ggplot(aes(x = regions, y = number_of_accounts)) +
  geom_bar(stat ="identity")+
  geom_text(aes(label=number_of_accounts), vjust=-0.3, size=3.5)+
  ggtitle("Number of active accounts of each region") +
  labs(y = "Number of active accounts", x = "Region name")


#MERGER
#Re-import 'accounts' dataset
accounts <- read.csv('accounts.csv')

accounts$account_created_time <- as.POSIXct(accounts$account_created_time, format = "%Y-%m-%d %H:%M:%S")
accounts$is_deleted <-  as.logical(accounts$is_deleted)
accounts$main_industry <- as.factor(accounts$main_industry)
accounts$sub_industry <- as.factor(accounts$sub_industry)
accounts$type <- as.factor(accounts$type)
accounts$support_tier <- as.factor(accounts$support_tier)
accounts <- accounts[,-c(4,5)]
accounts$month_yr <- as.factor(format(accounts$account_created_time, '%Y-%m'))
accounts$hour <- as.factor(format(accounts$account_created_time,'%H'))
accounts$account_created_date <- as.factor(format(accounts$account_created_time, '%Y-%m-%d'))

#Inner Join 2 datasets:
innerjoin <- merge(account_metrics,accounts, by = 'account_id')
nrow(innerjoin)

#Accounts with active projects or services
active_join <- innerjoin %>%
  filter(active_projects > 0 | active_services > 0) 
active_join

#Filter out the accounts created in 12th of March 2020
accounts <- accounts %>%
  filter(!(account_created_date == '2020-03-12'))

#Records with accounts created time after the latest service created time
innerjoin <- merge(account_metrics,accounts, by = 'account_id')
innerjoin %>%
  filter(latest_service_created < account_created_time)%>%
  nrow()

#records with accounts created time after the latest service created time
innerjoin %>%
  filter(latest_service_created > account_created_time)%>%
  nrow()




