# -*- coding: utf-8 -*-
"""genstat.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/14sJ5-QpEIZZp9TOw7Iy7umCIA2u3ITac
"""

"""
-------------------------------------------------------------------------------
AUTHOR:
Chenghong Wang (cw166@iu.edu)

COPYRIGHT:
MIT License

DESCRIPTION:
This script is used to generate differentially private (DP) synopses for given datasets.
It provides functionalities including filtering datasets, generating histograms,
and computing the maximum frequency of values, both in their exact and noisy forms.

FUNCTIONS:

- `generate_histogram(file_path, attribute, filter_type=None, filter_value=None, binnum=32)`:
  Computes the histogram for a specified attribute from a given CSV file. Filtering options are available.

- `gen_laplace_once(e, k)`:
  Generates one-sided Laplace noise based on parameters 'e' and 'k'.

- `noisy_histogram(file_path, attribute, e, filter_type=None, filter_value=None, binnum=32)`:
  Computes a noisy histogram for a specified attribute using Laplace noise.

- `compute_max_frequency(file_path, attribute, filter_type=None, filter_value=None)`:
  Computes the exact maximum frequency of values for a specified attribute from a given CSV file.

- `noisy_max_frequency(file_path, attribute, e, filter_type=None, filter_value=None)`:
  Computes the noisy maximum frequency of values for a specified attribute using Laplace noise.

-------------------------------------------------------------------------------
"""


from google.colab import drive
drive.mount('/content/drive')


import pandas as pd
import numpy as np
from math import ceil
import random


def count_tuples_with_condition(csv_path, attribute, value):
    data = pd.read_csv(csv_path)
    count = data[data[attribute] == value].shape[0]
    return count

#Generate one-sided laplace noise
def gen_laplace_once(e, k):
    u = np.random.geometric(1 - pow(2.713, -e), k)
    v = np.random.geometric(1 - pow(2.713, -e), k)
    return abs(u - v + np.full(k, 7))

def gen_negative_noise(e, k):
    """
    Generates negative Laplace noise with a mean of -10.

    Parameters:
    - e (float): Parameter for Laplace noise generation.
    - k (int): Number of noise values to generate.

    Returns:
    - (numpy.ndarray): Array of negative noise values.
    """
    u = np.random.geometric(1 - pow(2.713, -e), k)
    v = np.random.geometric(1 - pow(2.713, -e), k)
    return -abs(u - v - np.full(k, 7))


binnum = 16

def generate_histogram(file_path, attribute, filter_type=None, filter_value=None, binnum=8):
    """
    Generates a histogram for a given attribute from a CSV file.

    Parameters:
    - file_path (str): Path to the CSV file.
    - attribute (str): Attribute/column name for which histogram is needed.
    - filter_type (str, optional): Column name to filter the data (e.g. 'type'). Default is None.
    - filter_value (str, optional): Value to filter the data by (e.g. 'DISPONENT'). Default is None.
    - binnum (int, optional): Number of bins for the histogram. Default is 32.

    Returns:
    - hist (list): Histogram counts.
    """

    # Load the CSV data
    data = pd.read_csv(file_path)

    # Filter the data if filter_type and filter_value are specified
    if filter_type and filter_value:
        data = data[data[filter_type] == filter_value]

    # Determine min, max and bin width (rounded up)
    min_val = int(data[attribute].min())
    max_val = int(data[attribute].max())
    bin_width = ceil((max_val - min_val) / binnum)

    # Compute bin edges
    bin_edges = [min_val + i*bin_width for i in range(binnum+1)]  # We need binnum+1 edges for binnum bins

    # Compute histogram
    hist, _ = np.histogram(data[attribute], bins=bin_edges)

    return hist


def noisy_histogram(file_path, attribute, e, filter_type=None, filter_value=None, binnum=8):
    """
    Generates a noisy histogram using Laplace noise for a given attribute from a CSV file.

    Parameters:
    - file_path (str): Path to the CSV file.
    - attribute (str): Attribute/column name for which histogram is needed.
    - e (float): Parameter for Laplace noise generation.
    - filter_type (str, optional): Column name to filter the data (e.g. 'type'). Default is None.
    - filter_value (str, optional): Value to filter the data by (e.g. 'DISPONENT'). Default is None.
    - binnum (int, optional): Number of bins for the histogram. Default is 32.

    Returns:
    - noisy_histogram (list): Noisy histogram counts.
    """

    # Generate the histogram

    histogram = generate_histogram(file_path, attribute, filter_type, filter_value, binnum)

    # Convert the histogram to a numpy array
    histogram = np.array(histogram)

    # Add Laplace noise
    noisy_histogram = histogram + gen_laplace_once(e, binnum)

    return noisy_histogram.tolist()

def noisy_histogram_negative(file_path, attribute, e, filter_type=None, filter_value=None, binnum=8):
    """
    Generates a noisy histogram using negative Laplace noise for a given attribute from a CSV file.
    The histogram counts are ensured to be non-negative.

    Parameters:
    - file_path (str): Path to the CSV file.
    - attribute (str): Attribute/column name for which histogram is needed.
    - e (float): Parameter for Laplace noise generation.
    - filter_type (str, optional): Column name to filter the data (e.g. 'type'). Default is None.
    - filter_value (str, optional): Value to filter the data by (e.g. 'DISPONENT'). Default is None.
    - binnum (int, optional): Number of bins for the histogram. Default is 8.

    Returns:
    - noisy_histogram (list): Noisy histogram counts, with non-negative values.
    """

    # Generate the histogram
    histogram = generate_histogram(file_path, attribute, filter_type, filter_value, binnum)

    # Convert the histogram to a numpy array
    histogram = np.array(histogram)

    # Add negative Laplace noise
    noisy_histogram = histogram + gen_negative_noise(e, binnum)

    # Ensure non-negative histogram counts
    noisy_histogram = np.clip(noisy_histogram, 0, None)

    return noisy_histogram.tolist()


def compute_max_frequency(file_path, attribute, filter_type=None, filter_value=None):
    """
    Computes the exact maximum frequency of values under the specified attribute.

    Parameters:
    - file_path (str): Path to the CSV file.
    - attribute (str): Attribute/column name for which max frequency is computed.
    - filter_type (str, optional): Column name to filter the data (e.g. 'type'). Default is None.
    - filter_value (str, optional): Value to filter the data by (e.g. 'DISPONENT'). Default is None.

    Returns:
    - max_freq (int): Exact maximum frequency.
    """

    # Load the CSV data
    data = pd.read_csv(file_path)

    # Filter the data if filter_type and filter_value are specified
    if filter_type and filter_value:
        data = data[data[filter_type] == filter_value]

    # Compute max frequency
    max_freq = data[attribute].value_counts().max()

    return max_freq

def noisy_max_frequency(file_path, attribute, e, filter_type=None, filter_value=None):
    """
    Computes the noisy maximum frequency of values under the specified attribute.

    Parameters:
    - file_path (str): Path to the CSV file.
    - attribute (str): Attribute/column name for which max frequency is computed.
    - e (float): Parameter for Laplace noise generation.
    - filter_type (str, optional): Column name to filter the data (e.g. 'type'). Default is None.
    - filter_value (str, optional): Value to filter the data by (e.g. 'DISPONENT'). Default is None.

    Returns:
    - noisy_max_freq (int): Noisy maximum frequency.
    """

    # Compute the max frequency
    max_freq = compute_max_frequency(file_path, attribute, filter_type, filter_value)

    # Noisy max (TODO - this is bugged, should add noise then find max)
    noisy_max_freq = max_freq + np.random.exponential(scale=1/e, size=1)[0]
    return int(noisy_max_freq)

def compute_dp_indexes(true_histogram, noisy_histogram):
    """
    Computes DP indexes based on the true and noisy histograms.

    Parameters:
    - true_histogram (list of int): Actual histogram counts.
    - noisy_histogram (list of int): Noisy histogram counts.

    Returns:
    - indexes (list of tuples): Ordered list of (lo_i, hi_i) index pairs.
    """

    C = sum(true_histogram)
    binnum = len(noisy_histogram)

    # Initialize the list to store index pairs
    indexes = []

    # Calculate hi and lo for each bin
    for i in range(binnum):
        hi_i = min(sum(noisy_histogram[:i+1]), C)  # Calculate hi_i and clip to C if needed

        reverse_sum = sum(noisy_histogram[i:])
        lo_i = max(C - reverse_sum, 0)  # Calculate lo_i and clip to 0 if needed

        indexes.append((lo_i, hi_i))

    return indexes

def compute_cdf_indexes(positive_noisy_histogram, negative_noisy_histogram):
    """
    Computes CDF indexes using the true histogram and both positive and negative noisy histograms.

    Parameters:
    - positive_noisy_histogram (list of int): Noisy histogram counts using positive noise.
    - negative_noisy_histogram (list of int): Noisy histogram counts using negative noise.

    Returns:
    - indexes (list of tuples): Ordered list of (lo_i, hi_i) index pairs, where lo_i is
                                derived from the negative noisy histogram and hi_i from
                                the positive noisy histogram, with hi_i capped by the sum of the true histogram.
    """

    # Calculate the CDF for the positive noisy histogram
    cdf_positive = np.cumsum(positive_noisy_histogram)

    # Calculate the CDF for the negative noisy histogram
    cdf_negative = np.cumsum(negative_noisy_histogram)

    # Total count in the true histogram
    C = sum(list(positive_noisy_histogram))

    # Initialize the list to store index pairs
    indexes = []

    # Calculate lo_i and hi_i for each bin, capping hi_i with C
    for i in range(len(positive_noisy_histogram)):
        if (i == 0):
            lo_i = 0
            hi_i = min(cdf_positive[i], C)
        else:
            lo_i = cdf_negative[i-1]
            hi_i = min(cdf_positive[i], C)
        indexes.append((lo_i, hi_i))

    return indexes

def generate_synopsis(csv_path, target_attribute, epsilon=1, filter_attribute=None, filter_value=None):
    noisy_hist = noisy_histogram(csv_path, target_attribute, epsilon, filter_type=filter_attribute, filter_value=filter_value)
    noisy_neg_hist = noisy_histogram_negative(csv_path, target_attribute, epsilon, filter_type=filter_attribute, filter_value=filter_value)
    indexes = compute_cdf_indexes(noisy_hist, noisy_neg_hist)
    noisy_mf = noisy_max_frequency(csv_path, target_attribute, epsilon, filter_type=filter_attribute, filter_value=filter_value)

    return {
        "noisy_histogram": noisy_hist,
        "noisy_negative_histogram": noisy_neg_hist,
        "indexes": indexes,
        "noisy_max_frequency": noisy_mf
    }

def gen_account_synopses(csv_path):
    # Synopsis for account.id with filtering on district_id = 18
    synopsis_account_filtered = generate_synopsis(csv_path, 'account_id', filter_attribute='district_id', filter_value=18)

    # Synopsis for account over district_id for entire data
    synopsis_account_entire = generate_synopsis(csv_path, 'district_id')

    return {
        "account_filtered": synopsis_account_filtered,
        "account_entire": synopsis_account_entire
    }

def gen_client_synopses(csv_path):
    # Synopsis for account.id with filtering on district_id = 18
    synopsis_client_id = generate_synopsis(csv_path, 'client_id')

    # Synopsis for account over district_id for entire data
    synopsis_district_id = generate_synopsis(csv_path, 'district_id')

    return {
        "synopsis_client_id": synopsis_client_id,
        "synopsis_district_id": synopsis_district_id
    }


import pandas as pd

def join_tables(table1, table2, join_key, filter1=None, filter2=None, join_type='inner'):
    """
    Joins two tables based on a join key, with optional filters.

    Parameters:
    - table1_path (str): Path to the first CSV file.
    - table2_path (str): Path to the second CSV file.
    - join_key (str): The key on which to join the tables.
    - filter1 (dict, optional): Filters for the first table, in the format {column: value}.
    - filter2 (dict, optional): Filters for the second table, in the format {column: value}.
    - join_type (str, optional): Type of join operation ('inner', 'left', 'right', 'outer'). Default is 'inner'.

    Returns:
    - joined_table (DataFrame): The result of the join operation.
    - join_output_size (int): Number of rows in the joined table.
    """

    # Load the tables if they are file paths
    if isinstance(table1, str):
        table1 = pd.read_csv(table1)
    if isinstance(table2, str):
        table2 = pd.read_csv(table2)

    # Apply filters if provided
    if filter1:
        for key, value in filter1.items():
            table1 = table1[table1[key] == value]

    if filter2:
        for key, value in filter2.items():
            table2 = table2[table2[key] == value]

    print("table sizes:", len(table1), len(table2))

    # Perform the join operation
    joined_table = pd.merge(table1, table2, on=join_key, how=join_type)

    # Get the size of the joined table
    join_output_size = joined_table.shape[0]

    return joined_table, join_output_size

def exp_synop_generation(epsilon=1):

    # accounts
    account_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/account.csv', 'account_id', epsilon=epsilon, filter_attribute='district_id', filter_value=18)
    print("account:", account_synopsis)

    # transactions
    transaction_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/trans.csv', 'account_id', epsilon=epsilon, filter_attribute='operation', filter_value='VYBER KARTOU')
    print("trans:", transaction_synopsis)

    # order
    order_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/order.csv', 'account_id', epsilon=epsilon, filter_attribute='k_symbol', filter_value='LEASING')
    print("order 1:", order_synopsis)

    # order 2 (amount q2)
    order_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/order.csv', 'amount', epsilon=epsilon, filter_attribute='k_symbol', filter_value='LEASING')
    print("order 2:", order_synopsis)

    #client
    client_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/client.csv', 'client_id', epsilon=epsilon, filter_attribute='district_id', filter_value=18)
    print("client", client_synopsis)

    # disp
    disp_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/disp.csv', 'account_id', epsilon=epsilon, filter_attribute='type', filter_value='DISPONENT')
    print("disp:", disp_synopsis)

    # disp
    disp_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/disp.csv', 'account_id', epsilon=epsilon, filter_attribute=None, filter_value=None)
    print("disp_all:", disp_synopsis)

    #loan
    loan_synopsis = generate_synopsis('/content/drive/MyDrive/Colab Notebooks/financial/loan.csv', 'account_id', epsilon=epsilon, filter_attribute='duration', filter_value=36)
    print("loan:", loan_synopsis)

def q5_ground_truth():
    joined_table, size = join_tables(
        '/content/drive/MyDrive/Colab Notebooks/financial/disp.csv',
        '/content/drive/MyDrive/Colab Notebooks/financial/trans.csv',
        join_key='account_id',           # Assuming 'account_id' is the shared key for join
        filter1= None,     # Filter for the account table
        filter2={'operation': 'VYBER KARTOU'}, # Filter for the order table
        join_type='inner'                # Assuming you want an inner join
    )

    inter_joined_table, inter_size = join_tables(
        joined_table,                           # The result from the previous join
        '/content/drive/MyDrive/Colab Notebooks/financial/account.csv',  # Path to the order.csv file
        join_key='account_id',                  # Assuming 'account_id' is the shared key for join
        filter1=None,                           # No additional filter on the first table (joined_table)
        filter2={'district_id': 18},                           # No filter on the order table
        join_type='inner'                       # Inner join
    )

    final_joined_table, final_size = join_tables(
        inter_joined_table,                           # The result from the previous join
        '/content/drive/MyDrive/Colab Notebooks/financial/order.csv',  # Path to the order.csv file
        join_key='account_id',                  # Assuming 'account_id' is the shared key for join
        filter1=None,                           # No additional filter on the first table (joined_table)
        filter2={'k_symbol': 'LEASING'},                           # No filter on the order table
        join_type='inner'                       # Inner join
    )


    print("\nNumber of rows in the joined result:", size)
    print("\nNumber of rows in the joined result:", final_size)
    print(final_joined_table)

exp_synop_generation(epsilon=0.5)
#q5_ground_truth()