# -*- coding: utf-8 -*-
"""1024projectdatacleaning.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1_0Uup2AM15OqcD-Q7mnv3WHdZR76_qtZ
"""

import pandas as pd

df = pd.read_csv('/content/restaurant-menus.csv')
df

df1 = pd.read_csv('/content/restaurants.csv', skiprows=range(13748, None))
df1

df1 = pd.read_csv('/content/restaurants.csv')
df1

import pandas as pd

# Load your DataFrame (replace filename with your actual file path)
df = pd.read_csv('/content/restaurant-menus.csv')

# Example function to extract numeric values from price strings
def extract_price(price_string):
    try:
        numeric_part, currency = price_string.split()
        return float(numeric_part)
    except:
        return None

# Apply the function to the 'price' column
df['price'] = df['price'].apply(extract_price)

# Display the updated DataFrame
df

import csv

file_path = "/content/restaurant-menus.csv"

# Open the CSV file and read line by line
with open(file_path, 'r', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile)
    for line_number, row in enumerate(csvreader, start=1):
        try:
            # Process the row (you can add your own processing logic here)
            # For this example, we'll just print the row content
            print(f"Row {line_number}: {row}")
        except csv.Error as e:
            print(f"Error in line {line_number}: {e}")
            print("Row content:", row)

import csv

input_file_path = "/content/restaurant-menus.csv"
output_file_path = "/content/restaurant-menus-cleaned.csv"  # Specify the path for the cleaned CSV file

# Open the input CSV file for reading and the output CSV file for writing
with open(input_file_path, 'r', encoding='utf-8') as infile, \
        open(output_file_path, 'w', encoding='utf-8', newline='') as outfile:
    csvreader = csv.reader(infile)
    csvwriter = csv.writer(outfile)

    for line_number, row in enumerate(csvreader, start=1):
        try:
            # Process the row (you can add your own processing logic here)
            # For this example, we'll skip the problematic row
            if line_number != 538362:  # Specify the line number of the problematic row
                csvwriter.writerow(row)
        except csv.Error as e:
            print(f"Error in line {line_number}: {e}")
            print("Row content:", row)

import pandas as pd

# Load your DataFrame (replace filename with your actual file path)
df = pd.read_csv('/content/restaurant-menus-cleaned.csv')

# Example function to extract numeric values from price strings
def extract_price(price_string):
    try:
        numeric_part, currency = price_string.split()
        return float(numeric_part)
    except:
        return None

# Apply the function to the 'price' column
df['price'] = df['price'].apply(extract_price)

# Display the updated DataFrame
df

df1 = pd.read_csv("/content/restaurants.csv")
df1

#data cleaning

df1.isnull().sum()

df.isnull().sum()

score_NA = pd.isnull(df1["score"])
df1[score_NA]

rating_NA = pd.isnull(df1["ratings"])
df1[rating_NA]

category_NA = pd.isnull(df1["category"])
df1[category_NA]

pricerange_NA = pd.isnull(df1["price_range"])
df1[pricerange_NA]

zip_code_NA = pd.isnull(df1["zip_code"])
df1[zip_code_NA]

full_address_NA = pd.isnull(df1["full_address"])
df1[full_address_NA]

description_NA = pd.isnull(df["description"])
df1[description_NA]

df= df.dropna()
df

df1 = df1.dropna()
df1

df1 = df1.drop(columns=['price_range'])
df1

df1.isnull().sum()

df.isnull().sum()

df.rename(columns={'restaurant_id': 'id'}, inplace=True)
df

import pandas as pd

# Assuming you have defined the merged_df DataFrame

# Specify the file path where you want to save the CSV file
output_file_path = "/content/menu_dataframe.csv"

# Save the DataFrame to a CSV file
df.to_csv(output_file_path, index=False)

print("DataFrame saved to CSV:", output_file_path)

import pandas as pd

# Assuming you have defined the merged_df DataFrame

# Specify the file path where you want to save the CSV file
output_file_path = "/content/restaurant_dataframe.csv"

df1.to_csv(output_file_path, index=False)

print("DataFrame saved to CSV:", output_file_path)

joined_df = df.merge(df1, on='id', how='inner')
joined_df

joined_df.isnull().sum()

import pandas as pd

# Assuming you have defined the merged_df DataFrame

# Specify the file path where you want to save the CSV file
output_file_path = "/content/merged_dataframe.csv"

# Save the DataFrame to a CSV file
joined_df.to_csv(output_file_path, index=False)

print("DataFrame saved to CSV:", output_file_path)

joined_df = df.merge(df1, on='id', how='inner')

joined_df

print("df columns:", df.columns)
print("df1 columns:", df1.columns)

df = pd.read_csv('/content/restaurant_dataframe.csv')
df

df1 = pd.read_csv('/content/menu_dataframe.csv')
df1

df2 = pd.read_csv('/content/merged_dataframe.csv')
df2

import pandas as pd

# Assuming you have defined the 'df2' DataFrame

# Drop duplicates based on 'ratings' and 'restaurant_id' columns
distinct_ratings = df2.drop_duplicates(subset=['ratings', 'id'])

# Display the result
distinct_ratings

#df3 = df2.drop_duplicates(subset=['name_y','ratings'])

top_rated_restaurants = distinct_ratings.sort_values(by='ratings', ascending=False)

# Group by restaurant name and calculate the average score
grouped_by_name = top_rated_restaurants.groupby(['name_y','ratings']).mean()

# Convert the grouped Series to a DataFrame
grouped_df = grouped_by_name.reset_index()

# Select the top 20 rated restaurants
top_rated = grouped_df.sort_values(by='ratings', ascending=False).head(75)

# Display the top 20 rated restaurant names and scores
top_rated

import pandas as pd

# Assuming you have defined the 'top_rated_restaurants' DataFrame

# Specify the file path where you want to save the CSV file
output_file_path = "/content/top_rated_restaurants.csv"

# Save the DataFrame to a CSV file
top_rated.to_csv(output_file_path, index=False)

print("Top rated restaurants saved to CSV:", output_file_path)

import pandas as pd

# Assuming you have defined the 'distinct_ratings' DataFrame

# Select only the 'name' and 'ratings' columns
name_rating = top_rated[['name_y', 'ratings']]

# Display the result
print(name_rating)

# Create a new DataFrame with selected columns
name_rating_df = pd.DataFrame(name_rating)

# Display the new DataFrame
name_rating_df

import pandas as pd

# Assuming you have defined the 'distinct_ratings' DataFrame and created the 'name_rating_df' DataFrame

# Specify the file path where you want to save the CSV file
output_file_path = "/content/name_rating.csv"

# Save the new DataFrame to a CSV file
name_rating_df.to_csv(output_file_path, index=False)

print("DataFrame saved to CSV:", output_file_path)

import pandas as pd

# Assuming you have defined the 'name_rating_df' DataFrame

# Sort the DataFrame by 'ratings' column in descending order
name_rating_df_sorted = name_rating_df.sort_values(by='ratings', ascending=False)

# Display the sorted DataFrame
print(name_rating_df_sorted)



import pandas as pd

# Assuming you have defined the 'name_rating_df' DataFrame

# Sort the DataFrame by 'ratings' column in descending order
name_rating_df_sorted = name_rating_df.sort_values(by='ratings', ascending=False)

# Specify the file path where you want to save the CSV file
output_file_path = "/content/name_rating_sorted.csv"

# Save the sorted DataFrame to a CSV file
name_rating_df_sorted.to_csv(output_file_path, index=False)

print("Sorted DataFrame saved to CSV:", output_file_path)

import pandas as pd

# Assuming you have defined the 'df' DataFrame

# Use the 'value_counts()' method to get the counts of each category
category_counts = df2['category_y'].value_counts()

# Get the top three most common categories
top_10_categories = category_counts.head(10)

# Display the result
print(top_10_categories)

# Create a DataFrame from the top 10 categories and their counts
top_categories_df = pd.DataFrame({'Category': top_10_categories.index, 'Count': top_10_categories.values})

# Specify the file path where you want to save the CSV file
output_file_path = "/content/top_10_categories.csv"

# Save the DataFrame to a CSV file
top_categories_df.to_csv(output_file_path, index=False)

print("Top 10 categories saved to CSV:", output_file_path)

import pandas as pd

# Assuming you have defined the 'restaurants' and 'restaurant_menus' DataFrames

# Merge the two DataFrames on 'id' and 'restaurant_id'
#merged_df = pd.merge(restaurants, restaurant_menus, left_on='id', right_on='id')

# Group by restaurant name and count menu items
grouped_df = df2.groupby('name_y')['name_y'].count()

# Filter restaurants with more than 100 menu items
filtered_restaurants = grouped_df[grouped_df > 100]

# Create a new DataFrame with restaurant name and menu item count
result_df = pd.DataFrame({'restaurant_name': filtered_restaurants.index, 'menu_item_count': filtered_restaurants.values})

# Sort the result DataFrame by menu item count in descending order
result_df_sorted = result_df.sort_values(by='menu_item_count', ascending=False).head(50)

# Display the result
result_df_sorted

# Specify the file path where you want to save the CSV file
output_file_path = "/content/restaurant_menu_counts.csv"

# Save the result DataFrame to a CSV file
result_df_sorted.to_csv(output_file_path, index=False)

print("Result saved to CSV:", output_file_path)

# Group by restaurant name and calculate total branches, average rating, and average score
grouped_df = df2.groupby('name_y').agg(
    restaurant_name=pd.NamedAgg(column='name_y', aggfunc='first'),
    total_branches=pd.NamedAgg(column='full_address', aggfunc='count'),
    avg_rating=pd.NamedAgg(column='ratings', aggfunc='mean'),
    avg_score=pd.NamedAgg(column='score', aggfunc='mean')
)

# Sort the grouped DataFrame by total branches in descending order
sorted_df = grouped_df.sort_values(by='total_branches', ascending=False)

# Select the top 20 rows
top_20_restaurants = sorted_df.head(20)

# Specify the file path where you want to save the CSV file
output_file_path = "/content/top_20_restaurants.csv"

# Save the result DataFrame to a CSV file
top_20_restaurants.to_csv(output_file_path, index=False)

print("Top 20 restaurants saved to CSV:", output_file_path)

top_20_restaurants

import pandas as pd

# Assuming you have defined the 'restaurants' DataFrame

# Filter the DataFrame based on the specified zip code
filtered_restaurants = df2[df2['zip_code'] == '35208']

# Select the desired columns
result_df = filtered_restaurants[['name_y', 'full_address', 'zip_code']].head(100)

# Display the result
result_df

# Specify the file path where you want to save the CSV file
output_file_path = "/content/restaurants_with_zip_35208_limited.csv"

# Save the limited DataFrame to a CSV file
result_df.to_csv(output_file_path, index=False)

print("Limited DataFrame saved to CSV:", output_file_path)

import pandas as pd

# Assuming you have defined the 'restaurants' DataFrame

# Group the DataFrame by 'zip_code' and count the number of restaurants in each group
zip_code_counts = df2.groupby('zip_code').size().reset_index(name='restaurant_count').head(100)

# Display the result
zip_code_counts

# Specify the file path where you want to save the CSV file
output_file_path = "/content/restaurants_by_zipcode.csv"

# Save the limited DataFrame to a CSV file
zip_code_counts.to_csv(output_file_path, index=False)

print("Limited DataFrame saved to CSV:", output_file_path)

