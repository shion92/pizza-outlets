## Goal: 
We would like to know at any given point in time how many outlets are open. 

An example of the expected dataset for one of the outlets:

| SHOP_ID | STATUS | LOWER_RANGE | UPPER_RANGE |
| --- | --- | --- | --- |
| 5 | open | 1/01/2021 | 23/09/2021 |
| 5 | clsd | 24/09/2021 | 16/12/2021 |
| 5 | open | 17/12/2021 |  |

## Assumption/Limitation:

- An outlet is considered “closed” if it has had no transactions for 30 consecutive days.
- We assume the ‘current day’ to be 2022-12-31 (as we don't have any data beyond). That means the upper range = NULL in the latest status record for any shops in the final dataset.

## Workflow

1. Explore the dataset:  No missing values. Date range is from 2021-01-01 to 2022-12-31. `n_tran` is not relevant to the task.
2. Pre-processing: Load the CSV, sort the dataset, and set the date column to the correct datetime format (only in Python).
3. Add new columns: Create a ‘gap’ column, which is the number of days between records, and a ‘status’ column based on the ‘gap’.
4. Aggregation: Group by ‘shop_id’ and ‘status’, and calculate the minimum of ‘lower_range’ and the maximum of ‘upper_range’ to get row-level records of date range and status. At this step, ‘status’, ‘lower_range’, and ‘upper_range’ may not be entirely correct and need to be reviewed case by case.
5. Correct ‘status’, ‘lower_range’, and ‘upper_range’ case by case. In Python, wrap this in a `calculate_range()` function; in SQL, use `CASE WHEN`.
6. Remove unwanted rows and columns.
