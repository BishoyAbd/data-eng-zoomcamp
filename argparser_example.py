import argparse
import pandas as pd

def load_data(input_file):
    """Load data from a CSV file."""
    try:
        data = pd.read_csv(input_file)
        print(f"Loaded {len(data)} rows from {input_file}.")
        return data
    except Exception as e:
        print(f"Error loading data from {input_file}: {e}")
        exit(1)

def filter_data(data, column, value):
    """Filter the data by a specific column value."""
    if column not in data.columns:
        print(f"Column '{column}' not found in data.")
        exit(1)
    filtered_data = data[data[column] == value]
    print(f"Filtered data to {len(filtered_data)} rows based on {column}='{value}'.")
    return filtered_data

def save_data(data, output_file):
    """Save filtered data to a CSV file."""
    try:
        data.to_csv(output_file, index=False)
        print(f"Filtered data saved to {output_file}.")
    except Exception as e:
        print(f"Error saving data to {output_file}: {e}")
        exit(1)

def main():
    parser = argparse.ArgumentParser(description="Filter CSV data by column value.")
    parser.add_argument('--input', required=True, help="Path to the input CSV file.")
    parser.add_argument('--column', required=True, help="Column to filter by.")
    parser.add_argument('--value', required=True, help="Value to filter by.")
    parser.add_argument('--output', required=True, help="Path to save the output CSV file.")
    
    args = parser.parse_args()

    # Load, filter, and save data
    data = load_data(args.input)
    filtered_data = filter_data(data, args.column, args.value)
    save_data(filtered_data, args.output)

if __name__ == "__main__":
    main()
