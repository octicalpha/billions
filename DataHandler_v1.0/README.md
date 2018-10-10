External Website (data source)

|

|  DataCollector : save data as file

|

Internal Server

|

|  DataManager : parse file and save to DB

|

DBHandler : handle transactions to DB

|

|  DataManager : load data from DB

|

Alpha



It would be better to implement simple methods in DataHandler and have specific handlers inherit DataHandler.
(Not by getting DM as parameter)
