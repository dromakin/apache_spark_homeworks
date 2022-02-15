Expedia data. Expedia has provided you logs of customer behavior. These include what customers searched for, how they interacted with search results (click/book), whether or not the search result was a travel package. This data is a random selection from Expedia and is not representative of the overall statistics.

| Column name | Description | Data type |
| --- | --- | --- |
| id | ID of a data record | long |
| date_time | Timestamp | string |
| site_name | ID of the Expedia point of sale (i.e. Expedia.com, Expedia.co.uk, Expedia.co.jp, ...) | integer |
| posa_container | ID of continent associated with site | name | integer |
| user_location_country | The ID of the country the customer is located | integer |
| user_location_region | The ID of the region the customer is located | integer |
| user_location_city | The ID of the city the customer is located | integer |
| orig_destination_distance | Physical distance between a hotel and a customer at the time of search. A null means the distance could not be calculated | double |
| user_id | ID of user | integer |
| is_mobile | 1 when a user connected from a mobile device, 0 otherwise | integer |
| is_package | 1 if the click/booking was generated as a part of a package (i.e. combined with a flight), 0 otherwise | integer |
| channel | ID of a marketing channel | integer |
| srch_ci | Checkin date | string |
| srch_o | Checkout date | string |
| srch_adults_cnt | The number of adults specified in the hotel room | integer |
| srch_children_cnt | The number of (extra occupancy) children specified in the hotel room | integer |
| srch_rm_cnt | The number of hotel rooms specified in the search | integer |
| srch_destination_id | ID of the destination where the hotel search was performed | integer |
| srch_destination_type_id | type | id | Type of destination | integer |
| hotel_id | ID of hotel | long |


Below is description of Kafka topic, where "expedia" - is a topic name; partition=0, partition=1, partition=2 - is a partition number.
topics/expedia/partition=0
topics/expedia/partition=1
topics/expedia/partition=2